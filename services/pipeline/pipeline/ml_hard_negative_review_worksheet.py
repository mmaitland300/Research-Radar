"""Reviewer-blind hard-negative / near-miss worksheet.

Read-only DB helper for collecting relevance-boundary labels. It deliberately
keeps ranking and feature context out of the reviewer CSV and stores it only in
the row_id-keyed sidecar.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_blind_snapshot_review_worksheet import (
    ABSTRACT_PREVIEW_MAX_CHARS,
    MAX_ROWS,
    MIN_ROWS,
    BlindCandidate,
    _citation_band,
    _truncate_abstract,
    _year_band,
    assert_succeeded_clustering_run,
    assert_succeeded_ranking_run,
    fetch_candidate_pool,
    fetch_ranking_context,
    load_label_dataset_payload,
    raw_pool_to_candidates,
)
from pipeline.ml_blind_snapshot_review_worksheet_v2 import fetch_paper_score_feature_context
from pipeline.ml_label_dataset import LABEL_FIELDS, paper_id_to_work_id, sha256_file
from pipeline.repo_paths import portable_repo_path

WORKSHEET_VERSION = "ml-hard-negative-review-v1"
REVIEW_POOL_VARIANT = "ml_hard_negative_audit"
DEFAULT_SAMPLE_SEED = 20260513

ALLOWED_SAMPLE_REASONS: tuple[str, ...] = (
    "off_slice_topic_metadata",
    "weak_music_audio_context",
    "broad_audio_not_mir",
    "education_or_health_surface_match",
    "industrial_or_bioacoustic_surface_match",
    "low_family_score_near_miss",
    "lexical_music_surface_match",
    "fallback_seeded_fill",
)

CSV_COLUMNS: tuple[str, ...] = (
    "row_id",
    "worksheet_version",
    "review_pool_variant",
    "paper_id",
    "openalex_work_id",
    "work_id",
    "title",
    "year",
    "citation_count",
    "source_slug",
    "topics",
    "abstract_preview",
    "sample_reason",
    "cluster_id",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)

HIDDEN_REVIEWER_CSV_FIELDS: tuple[str, ...] = (
    "ranking_run_id",
    "ranking_run_id_context",
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
    "internal_work_id",
    "final_score",
    "semantic_score",
    "citation_velocity_score",
    "topic_growth_score",
    "diversity_penalty",
    "bridge_score",
    "ranking_context_family_scores_json",
    "ranking_context_family_ranks_json",
    "learned_logit",
    "model_prediction",
)

CAVEATS: tuple[str, ...] = (
    "This worksheet is not validation.",
    "Rows are for offline hard-negative / near-miss manual labeling only.",
    "The reviewer CSV intentionally hides ranking scores, ranks, family-score JSON, model predictions, and global snapshot/version strings.",
    "No model is trained, no ranking is run, and no production ranking change is supported.",
)

MUSIC_TERMS = (
    "music",
    "musical",
    "song",
    "songs",
    "playlist",
    "melody",
    "harmony",
    "instrument",
    "vocal",
    "singing",
    "genre",
    "mir",
)
MIR_CORE_TERMS = (
    "music information retrieval",
    "music retrieval",
    "music recommendation",
    "music recommender",
    "music tagging",
    "music transcription",
    "source separation",
    "music generation",
    "audio to score",
    "symbolic music",
    "computational musicology",
)
AUDIO_TERMS = (
    "audio",
    "sound",
    "acoustic",
    "speech",
    "noise",
    "hearing",
    "binaural",
    "loudspeaker",
    "headphone",
)
EDU_HEALTH_TERMS = (
    "education",
    "teaching",
    "classroom",
    "student",
    "learning interaction",
    "therapy",
    "health",
    "patient",
    "wellness",
    "clinical",
)
INDUSTRIAL_BIOACOUSTIC_TERMS = (
    "environmental sound",
    "urban sound",
    "sound event",
    "traffic",
    "vehicle",
    "industrial",
    "machine fault",
    "machinery",
    "bioacoustic",
    "biological sound",
)


class MLHardNegativeReviewWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class HardNegativeSelection:
    candidate: BlindCandidate
    sample_reason: str
    signals: tuple[str, ...]
    auxiliary_scores: dict[str, Any]


def stable_row_id(*, worksheet_version: str, sample_seed: int, paper_id: str) -> str:
    raw = f"{worksheet_version}|{sample_seed}|{paper_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _norm_ws(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _has_any(values: Sequence[str], blob: str) -> bool:
    return any(v in blob for v in values)


def _candidate_blob(cand: BlindCandidate) -> str:
    return " ".join(
        [
            cand.title,
            cand.abstract,
            " ".join(cand.topics),
            cand.source_slug,
            cand.work_type,
        ]
    ).lower()


def any_labeled_work_tokens(payload: dict[str, Any]) -> set[str]:
    """Uppercase W tokens with any non-empty manual label in v5."""
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return set()
    out: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not any(_norm_ws(row.get(field)) for field in LABEL_FIELDS):
            continue
        for raw in (row.get("work_id"), row.get("openalex_work_id"), row.get("paper_id")):
            token = paper_id_to_work_id(_norm_ws(raw))
            if token:
                out.add(token.upper())
    return out


def _emerging_features(
    cand: BlindCandidate,
    score_feature_context: dict[int, dict[str, dict[str, Any]]],
) -> dict[str, Any] | None:
    return score_feature_context.get(cand.internal_work_id, {}).get("emerging")


def _score_value(features: dict[str, Any] | None, field: str) -> float | None:
    if not features:
        return None
    raw = features.get(field)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _classify_candidate(
    cand: BlindCandidate,
    *,
    score_feature_context: dict[int, dict[str, dict[str, Any]]],
    low_score_threshold: float | None,
) -> tuple[str, tuple[str, ...], dict[str, Any]]:
    blob = _candidate_blob(cand)
    emerging = _emerging_features(cand, score_feature_context)
    final_score = _score_value(emerging, "final_score")
    semantic_score = _score_value(emerging, "semantic_score")
    topic_growth_score = _score_value(emerging, "topic_growth_score")

    has_music = _has_any(MUSIC_TERMS, blob)
    has_audio = _has_any(AUDIO_TERMS, blob)
    has_core_mir = _has_any(MIR_CORE_TERMS, blob)
    has_edu_health = _has_any(EDU_HEALTH_TERMS, blob)
    has_industrial = _has_any(INDUSTRIAL_BIOACOUSTIC_TERMS, blob)
    low_score = final_score is not None and low_score_threshold is not None and final_score <= low_score_threshold
    off_slice_topic = bool(cand.topics) and not any(
        _has_any(MUSIC_TERMS + AUDIO_TERMS, topic.lower()) for topic in cand.topics
    )

    signals: list[str] = []
    if has_music:
        signals.append("lexical_music")
    if has_audio:
        signals.append("lexical_audio")
    if has_core_mir:
        signals.append("core_mir_hook")
    if has_edu_health:
        signals.append("education_or_health_terms")
    if has_industrial:
        signals.append("industrial_or_bioacoustic_terms")
    if low_score:
        signals.append("low_emerging_final_score")
    if off_slice_topic:
        signals.append("off_slice_topic_metadata")

    auxiliary_scores = {
        "emerging_final_score": final_score,
        "emerging_semantic_score": semantic_score,
        "emerging_topic_growth_score": topic_growth_score,
        "low_score_threshold": low_score_threshold,
        "has_music_terms": has_music,
        "has_audio_terms": has_audio,
        "has_core_mir_terms": has_core_mir,
        "topic_names": list(cand.topics),
    }

    if has_industrial and not has_core_mir:
        return "industrial_or_bioacoustic_surface_match", tuple(signals), auxiliary_scores
    if has_edu_health:
        return "education_or_health_surface_match", tuple(signals), auxiliary_scores
    if has_audio and not has_music:
        return "broad_audio_not_mir", tuple(signals), auxiliary_scores
    if off_slice_topic and (has_music or has_audio):
        return "off_slice_topic_metadata", tuple(signals), auxiliary_scores
    if low_score and (has_music or has_audio):
        return "low_family_score_near_miss", tuple(signals), auxiliary_scores
    if has_music and not has_core_mir:
        return "weak_music_audio_context", tuple(signals), auxiliary_scores
    if has_music:
        return "lexical_music_surface_match", tuple(signals), auxiliary_scores
    return "fallback_seeded_fill", tuple(signals), auxiliary_scores


def _low_score_threshold(
    candidates: Sequence[BlindCandidate],
    *,
    score_feature_context: dict[int, dict[str, dict[str, Any]]],
) -> float | None:
    scores = sorted(
        score
        for cand in candidates
        if (score := _score_value(_emerging_features(cand, score_feature_context), "final_score")) is not None
    )
    if not scores:
        return None
    # Bottom 40% creates a near-miss pool while avoiding score ordering in the sheet.
    idx = max(0, min(len(scores) - 1, int((len(scores) - 1) * 0.40)))
    return float(scores[idx])


def _seeded_shuffle(seq: Sequence[Any], *, seed: int, salt: str) -> list[Any]:
    out = list(seq)
    random.Random(f"{seed}|{salt}").shuffle(out)
    return out


def select_hard_negative_sample(
    pool: Sequence[BlindCandidate],
    *,
    any_labeled_tokens: set[str],
    score_feature_context: dict[int, dict[str, dict[str, Any]]],
    total_rows: int,
    seed: int,
) -> tuple[list[HardNegativeSelection], dict[str, Any]]:
    if total_rows < MIN_ROWS or total_rows > MAX_ROWS:
        raise MLHardNegativeReviewWorksheetError(f"--rows must be between {MIN_ROWS} and {MAX_ROWS}")

    eligible = [cand for cand in pool if cand.work_token.upper() not in any_labeled_tokens]
    threshold = _low_score_threshold(eligible, score_feature_context=score_feature_context)
    annotated = [
        HardNegativeSelection(
            candidate=cand,
            sample_reason=reason,
            signals=signals,
            auxiliary_scores=aux,
        )
        for cand in eligible
        for reason, signals, aux in [_classify_candidate(cand, score_feature_context=score_feature_context, low_score_threshold=threshold)]
    ]
    credible = [item for item in annotated if item.sample_reason != "fallback_seeded_fill"]
    fallback = [item for item in annotated if item.sample_reason == "fallback_seeded_fill"]

    target = min(total_rows, len(credible))
    by_reason: dict[str, list[HardNegativeSelection]] = defaultdict(list)
    for item in credible:
        by_reason[item.sample_reason].append(item)
    for reason in ALLOWED_SAMPLE_REASONS:
        by_reason[reason] = _seeded_shuffle(by_reason.get(reason, []), seed=seed, salt=f"reason:{reason}")

    selected: list[HardNegativeSelection] = []
    used: set[str] = set()
    reason_order = tuple(r for r in ALLOWED_SAMPLE_REASONS if r != "fallback_seeded_fill")
    while len(selected) < target:
        progressed = False
        for reason in reason_order:
            if len(selected) >= target:
                break
            bucket = by_reason.get(reason, [])
            while bucket:
                item = bucket.pop(0)
                token = item.candidate.work_token.upper()
                if token in used:
                    continue
                selected.append(item)
                used.add(token)
                progressed = True
                break
        if not progressed:
            break

    selected = _seeded_shuffle(selected, seed=seed, salt="csv_order")
    reason_counts = Counter(item.sample_reason for item in selected)
    debug = {
        "requested_rows": total_rows,
        "target_rows": target,
        "achieved_rows": len(selected),
        "shortfall_count": max(0, total_rows - len(selected)),
        "raw_candidate_pool_size": len(pool),
        "eligible_pool_size": len(eligible),
        "any_labeled_excluded_count": sum(1 for cand in pool if cand.work_token.upper() in any_labeled_tokens),
        "credible_candidate_pool_size": len(credible),
        "fallback_candidate_pool_size": len(fallback),
        "low_score_threshold": threshold,
        "sample_reason_counts": dict(sorted(reason_counts.items())),
        "cluster_ids": sorted({item.candidate.cluster_id for item in selected}),
        "pool_supported_requested_rows": len(credible) >= total_rows,
        "selection_note": (
            "Requested row count was supported by credible hard-negative / near-miss candidates."
            if len(credible) >= total_rows
            else "Shortfall: conservative v5 exclusion left fewer credible hard-negative / near-miss candidates than requested."
        ),
    }
    return selected, debug


def _family_json(data: dict[str, Any]) -> str:
    if not data:
        return ""
    return json.dumps({k: data[k] for k in sorted(data)}, sort_keys=True)


def _candidate_csv_row(*, selection: HardNegativeSelection, seed: int) -> dict[str, str]:
    cand = selection.candidate
    return {
        "row_id": stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id),
        "worksheet_version": WORKSHEET_VERSION,
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "work_id": cand.work_token,
        "title": cand.title,
        "year": str(int(cand.year)) if cand.year is not None else "",
        "citation_count": str(int(cand.citation_count)),
        "source_slug": cand.source_slug,
        "topics": ";".join(cand.topics) if cand.topics else "",
        "abstract_preview": _truncate_abstract(cand.abstract, ABSTRACT_PREVIEW_MAX_CHARS) if cand.abstract else "",
        "sample_reason": selection.sample_reason,
        "cluster_id": cand.cluster_id,
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def _candidate_sidecar_row(
    *,
    selection: HardNegativeSelection,
    seed: int,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
    score_feature_context: dict[int, dict[str, dict[str, Any]]],
) -> dict[str, Any]:
    cand = selection.candidate
    family_features = score_feature_context.get(cand.internal_work_id, {})
    return {
        "row_id": stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id),
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "internal_work_id": cand.internal_work_id,
        "sample_seed": seed,
        "sample_reason": selection.sample_reason,
        "hard_negative_signals": list(selection.signals),
        "selection_auxiliary_scores": selection.auxiliary_scores,
        "cluster_id": cand.cluster_id,
        "corpus_snapshot_version": corpus_snapshot_version,
        "embedding_version": embedding_version,
        "cluster_version": cluster_version,
        "ranking_run_id": ranking_run_id,
        "ranking_context_family_scores_json": _family_json(cand.family_scores),
        "ranking_context_family_ranks_json": _family_json(cand.family_ranks),
        "emerging_paper_scores": family_features.get("emerging"),
        "paper_scores_by_family": {fam: family_features[fam] for fam in sorted(family_features)},
    }


def render_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})
    return buf.getvalue()


def build_context_payload(
    *,
    sidecar_rows: Sequence[dict[str, Any]],
    label_dataset_path: Path,
    label_dataset_sha256: str,
    conflict_policy_path: Path,
    conflict_policy_sha256: str,
    requested_rows: int,
    debug: dict[str, Any],
    seed: int,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
) -> dict[str, Any]:
    return {
        "artifact_type": "ml_hard_negative_review_v1_context",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "provenance": {
            "worksheet_version": WORKSHEET_VERSION,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "sample_seed": seed,
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "label_dataset_path": portable_repo_path(label_dataset_path),
            "label_dataset_sha256": label_dataset_sha256,
            "conflict_policy_path": portable_repo_path(conflict_policy_path),
            "conflict_policy_sha256": conflict_policy_sha256,
            "ranking_run_id": ranking_run_id,
            "corpus_snapshot_version": corpus_snapshot_version,
            "embedding_version": embedding_version,
            "cluster_version": cluster_version,
            "requested_rows": requested_rows,
            "achieved_rows": int(debug.get("achieved_rows", len(sidecar_rows))),
        },
        "schema": {
            "key": "row_id",
            "hidden_from_reviewer_csv": list(HIDDEN_REVIEWER_CSV_FIELDS),
            "notes": (
                "Reviewer CSV stores OpenAlex W tokens in work_id. Postgres works.id and all ranking/score "
                "context appear only in this sidecar."
            ),
        },
        "sampling_policy": {
            "exclusion_rule": (
                "Exclude a work if any v5 row for that OpenAlex work has any non-empty manual label among "
                "relevance_label, novelty_label, or bridge_like_label."
            ),
            "allowed_sample_reasons": list(ALLOWED_SAMPLE_REASONS),
            "csv_ordering": "seeded hash shuffle over selected rows; not final_score order",
        },
        "sampling_debug": dict(debug),
        "rows": list(sidecar_rows),
    }


def render_markdown(
    *,
    selected: Sequence[HardNegativeSelection],
    debug: dict[str, Any],
    seed: int,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
    label_dataset_path: Path,
    label_dataset_sha256: str,
    conflict_policy_path: Path,
    conflict_policy_sha256: str,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    requested_rows: int,
) -> str:
    by_reason: Counter[str] = Counter(item.sample_reason for item in selected)
    by_cluster: Counter[str] = Counter(item.candidate.cluster_id for item in selected)
    by_year_band: Counter[str] = Counter(_year_band(item.candidate.year) for item in selected)
    by_cite_band: Counter[str] = Counter(_citation_band(item.candidate.citation_count) for item in selected)
    achieved = int(debug.get("achieved_rows", len(selected)))
    shortfall = int(debug.get("shortfall_count", max(0, requested_rows - achieved)))
    supported = bool(debug.get("pool_supported_requested_rows", False))
    lines = [
        f"# Hard-negative review worksheet (`{WORKSHEET_VERSION}`)",
        "",
        "## Purpose",
        "",
        "Reviewer-blind worksheet for deliberate negative and borderline relevance-boundary examples. "
        "This is a label-quality audit artifact, not model training, ranking, validation, or production readiness.",
        "",
        "## Provenance",
        "",
        f"- **worksheet_version:** `{WORKSHEET_VERSION}`",
        f"- **review_pool_variant:** `{REVIEW_POOL_VARIANT}`",
        f"- **sample_seed:** `{seed}`",
        f"- **row_id formula:** `sha256(worksheet_version|sample_seed|paper_id)`",
        f"- **label_dataset:** `{portable_repo_path(label_dataset_path)}`",
        f"- **label_dataset_sha256:** `{label_dataset_sha256}`",
        f"- **conflict_policy:** `{portable_repo_path(conflict_policy_path)}`",
        f"- **conflict_policy_sha256:** `{conflict_policy_sha256}`",
        f"- **ranking_run_id:** `{ranking_run_id}`",
        f"- **corpus_snapshot_version:** `{corpus_snapshot_version}`",
        f"- **embedding_version:** `{embedding_version}`",
        f"- **cluster_version:** `{cluster_version}`",
        f"- **csv_output:** `{portable_repo_path(csv_output_path)}`",
        f"- **context_sidecar_output:** `{portable_repo_path(context_output_path)}`",
        f"- **markdown_output:** `{portable_repo_path(markdown_output_path)}`",
        "",
        "## Reviewer CSV Policy",
        "",
        "The reviewer CSV excludes `ranking_run_id`, `internal_work_id`, score/rank fields, family score/rank JSON, learned logits, model predictions, and the global `corpus_snapshot_version`, `embedding_version`, and `cluster_version` strings. "
        "Those fields are preserved in the sidecar and this Markdown only.",
        "",
        "## Exclusion Rule",
        "",
        "A work is excluded if any row in the v5 label dataset for the same OpenAlex work already has at least one non-empty manual label field among `relevance_label`, `novelty_label`, or `bridge_like_label`.",
        "",
        "## Sample Summary",
        "",
        f"- **requested rows:** `{requested_rows}`",
        f"- **achieved rows:** `{achieved}`",
        f"- **shortfall:** `{shortfall}`",
        f"- **raw candidate pool size:** `{debug.get('raw_candidate_pool_size')}`",
        f"- **eligible after any-label exclusion:** `{debug.get('eligible_pool_size')}`",
        f"- **excluded by any-label rule:** `{debug.get('any_labeled_excluded_count')}`",
        f"- **credible hard-negative / near-miss candidates:** `{debug.get('credible_candidate_pool_size')}`",
        f"- **pool supported requested hard-negative intent:** `{str(supported).lower()}`",
        f"- **selection note:** {debug.get('selection_note')}",
    ]
    if shortfall > 0:
        lines.extend(
            [
                "",
                "## Shortfall",
                "",
                f"The worksheet intentionally emits {achieved} rows instead of padding to {requested_rows}. "
                "The conservative v5 exclusion rule leaves too few credible hard-negative / near-miss candidates in the current curated snapshot.",
            ]
        )
    lines.extend(
        [
            "",
            "## Row Counts By Sample Reason",
            "",
            "| sample_reason | rows |",
            "|---|---:|",
            *[f"| `{reason}` | {by_reason[reason]} |" for reason in ALLOWED_SAMPLE_REASONS if by_reason[reason] > 0],
            "",
            "## Cluster Coverage",
            "",
            "| cluster_id | rows |",
            "|---|---:|",
            *[f"| `{cluster}` | {by_cluster[cluster]} |" for cluster in sorted(by_cluster)],
            "",
            "## Year Bands",
            "",
            "| year_band | rows |",
            "|---|---:|",
            *[f"| `{band}` | {by_year_band[band]} |" for band in sorted(by_year_band)],
            "",
            "## Citation Bands",
            "",
            "| citation_band | rows |",
            "|---|---:|",
            *[f"| `{band}` | {by_cite_band[band]} |" for band in sorted(by_cite_band)],
            "",
            "## Future Ingest Note",
            "",
            "When labeled, a later dataset ingest should merge the sidecar by `row_id` and keep `review_pool_variant=ml_hard_negative_audit` distinct unless an experiment explicitly pools it with blind snapshot rows.",
            "",
            "## Caveats",
            "",
            *[f"- {c}" for c in CAVEATS],
            "",
        ]
    )
    return "\n".join(lines)


def build_hard_negative_review_worksheet(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
    rows: int,
    seed: int,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
) -> tuple[str, dict[str, Any], str, dict[str, Any]]:
    if rows < MIN_ROWS or rows > MAX_ROWS:
        raise MLHardNegativeReviewWorksheetError(f"--rows must be between {MIN_ROWS} and {MAX_ROWS}")
    if not label_dataset_path.is_file():
        raise MLHardNegativeReviewWorksheetError(f"label dataset not found: {label_dataset_path}")
    if not conflict_policy_path.is_file():
        raise MLHardNegativeReviewWorksheetError(f"conflict policy not found: {conflict_policy_path}")

    label_payload = load_label_dataset_payload(label_dataset_path)
    label_sha = sha256_file(label_dataset_path)
    conflict_sha = sha256_file(conflict_policy_path)
    excluded_tokens = any_labeled_work_tokens(label_payload)

    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        assert_succeeded_clustering_run(
            conn,
            cluster_version=cluster_version,
            expected_corpus_snapshot_version=corpus_snapshot_version,
            expected_embedding_version=embedding_version,
        )
        assert_succeeded_ranking_run(
            conn,
            ranking_run_id=ranking_run_id,
            expected_corpus_snapshot_version=corpus_snapshot_version,
            expected_embedding_version=embedding_version,
        )
        raw_rows = fetch_candidate_pool(
            conn,
            corpus_snapshot_version=corpus_snapshot_version,
            cluster_version=cluster_version,
        )
        ranking_context = fetch_ranking_context(conn, ranking_run_id=ranking_run_id)
        score_feature_context = fetch_paper_score_feature_context(conn, ranking_run_id=ranking_run_id)

    candidates = raw_pool_to_candidates(raw_rows, ranking_context=ranking_context)
    selected, debug = select_hard_negative_sample(
        candidates,
        any_labeled_tokens=excluded_tokens,
        score_feature_context=score_feature_context,
        total_rows=rows,
        seed=seed,
    )
    csv_rows = [_candidate_csv_row(selection=item, seed=seed) for item in selected]
    sidecar_rows = [
        _candidate_sidecar_row(
            selection=item,
            seed=seed,
            corpus_snapshot_version=corpus_snapshot_version,
            embedding_version=embedding_version,
            cluster_version=cluster_version,
            ranking_run_id=ranking_run_id,
            score_feature_context=score_feature_context,
        )
        for item in selected
    ]
    csv_text = render_csv(csv_rows)
    context_payload = build_context_payload(
        sidecar_rows=sidecar_rows,
        label_dataset_path=label_dataset_path,
        label_dataset_sha256=label_sha,
        conflict_policy_path=conflict_policy_path,
        conflict_policy_sha256=conflict_sha,
        requested_rows=rows,
        debug=debug,
        seed=seed,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        cluster_version=cluster_version,
        ranking_run_id=ranking_run_id,
    )
    md_text = render_markdown(
        selected=selected,
        debug=debug,
        seed=seed,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        cluster_version=cluster_version,
        ranking_run_id=ranking_run_id,
        label_dataset_path=label_dataset_path,
        label_dataset_sha256=label_sha,
        conflict_policy_path=conflict_policy_path,
        conflict_policy_sha256=conflict_sha,
        csv_output_path=csv_output_path,
        context_output_path=context_output_path,
        markdown_output_path=markdown_output_path,
        requested_rows=rows,
    )
    return csv_text, context_payload, md_text, debug


def run_ml_hard_negative_review_worksheet_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
    rows: int,
    seed: int,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
) -> dict[str, Any]:
    csv_text, context_payload, md_text, debug = build_hard_negative_review_worksheet(
        database_url=database_url,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        cluster_version=cluster_version,
        ranking_run_id=ranking_run_id,
        rows=rows,
        seed=seed,
        csv_output_path=csv_output_path,
        context_output_path=context_output_path,
        markdown_output_path=markdown_output_path,
    )
    csv_output_path.parent.mkdir(parents=True, exist_ok=True)
    context_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    csv_output_path.write_text(csv_text, encoding="utf-8", newline="")
    context_output_path.write_text(json.dumps(context_payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    markdown_output_path.write_text(md_text, encoding="utf-8", newline="")
    return debug


__all__ = [
    "ALLOWED_SAMPLE_REASONS",
    "CSV_COLUMNS",
    "DEFAULT_SAMPLE_SEED",
    "HIDDEN_REVIEWER_CSV_FIELDS",
    "MLHardNegativeReviewWorksheetError",
    "REVIEW_POOL_VARIANT",
    "WORKSHEET_VERSION",
    "any_labeled_work_tokens",
    "build_hard_negative_review_worksheet",
    "render_csv",
    "render_markdown",
    "run_ml_hard_negative_review_worksheet_cli",
    "select_hard_negative_sample",
    "stable_row_id",
]
