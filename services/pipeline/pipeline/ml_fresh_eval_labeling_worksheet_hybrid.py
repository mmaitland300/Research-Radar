"""Fresh hybrid eval manual-labeling worksheet generator.

This creates reviewer-blank CSV rows plus a row_id-keyed context sidecar for
the fresh hybrid confirmation surface. It does not ingest labels, score, train,
rank, embed, validate hybrids, or authorize shadow/prod.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import random
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg

from pipeline.ml_fresh_hybrid_candidate_plan_ingest import (
    MLFreshHybridCandidatePlanIngestError,
    assert_local_database_url,
)
from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_eval_labeling_worksheet_hybrid"
WORKSHEET_VERSION = "ml-fresh-eval-labeling-worksheet-hybrid-v1"
DEFAULT_REVIEW_POOL_VARIANT = "ml_fresh_hybrid_eval_v1"
DEFAULT_ROWS = 120
DEFAULT_SEED = 20260519
SURFACE_ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
TARGET = "good_or_acceptable"

RECOMMENDED_NEXT_STAGE_VALUES = {
    "create_fresh_eval_labeling_worksheet_hybrid_v1",
    "create_fresh_eval_labeling_plan_hybrid_v1",
}

CSV_COLUMNS = (
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
    "ranking_run_id",
    "family",
    "final_score",
    "rank_in_family",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)

BLANK_LABEL_COLUMNS = ("relevance_label", "novelty_label", "bridge_like_label", "reviewer_notes")

ALLOWED_LABEL_SETS = {
    "relevance_label": ["good", "acceptable", "miss", "irrelevant"],
    "novelty_label": ["surprising", "useful", "obvious", "not_useful", "neither"],
    "bridge_like_label": ["yes", "partial", "no", "not_applicable"],
}

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)

CAVEATS = (
    "Manual labeling worksheet only; no labels are ingested by this command.",
    "Rows are future confirmatory eval candidates, not validation results.",
    "Old 217-work overlaps and existing v8-labeled canonical works are excluded by default.",
    "Reviewer label columns are intentionally blank.",
    "No scoring, training, ranking, embeddings, hybrid validation, shadow, or production authorization.",
)


class MLFreshEvalLabelingWorksheetHybridError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshEvalLabelingWorksheetHybridError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshEvalLabelingWorksheetHybridError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshEvalLabelingWorksheetHybridError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLFreshEvalLabelingWorksheetHybridError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _canonical_work_id_from_label(row: Mapping[str, Any]) -> str | None:
    for field in ("work_id", "openalex_work_id", "paper_id"):
        canonical = _canonical_from_value(row.get(field))
        if canonical:
            return canonical
    return None


def _assert_local_database_url(database_url: str) -> dict[str, Any]:
    try:
        return assert_local_database_url(database_url)
    except MLFreshHybridCandidatePlanIngestError as exc:
        raise MLFreshEvalLabelingWorksheetHybridError(str(exc), code=exc.code) from exc


def stable_row_id(*, worksheet_version: str, seed: int, canonical_openalex_work_id: str) -> str:
    raw = f"{worksheet_version}|{seed}|{canonical_openalex_work_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _validate_surface(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="fresh-eval-surface")
    if metadata.get("artifact_type") != SURFACE_ARTIFACT_TYPE:
        raise MLFreshEvalLabelingWorksheetHybridError("fresh surface metadata.artifact_type mismatch")
    if metadata.get("surface_version") != SURFACE_VERSION:
        raise MLFreshEvalLabelingWorksheetHybridError("surface_version must be ml-fresh-eval-surface-hybrid-v1")
    if metadata.get("status") != "materialized_needs_labels":
        raise MLFreshEvalLabelingWorksheetHybridError("fresh surface status must be materialized_needs_labels")
    if payload.get("ready_for_hybrid_validation_scoring") is not False:
        raise MLFreshEvalLabelingWorksheetHybridError("ready_for_hybrid_validation_scoring must be false")
    if payload.get("recommended_next_stage") not in RECOMMENDED_NEXT_STAGE_VALUES:
        raise MLFreshEvalLabelingWorksheetHybridError(
            "fresh surface recommended_next_stage must route to fresh eval labeling worksheet/plan"
        )
    eligible = _get(payload, "confirmatory_eligibility.confirmatory_metric_eligible_work_count")
    if not isinstance(eligible, int) or isinstance(eligible, bool) or eligible < 100:
        raise MLFreshEvalLabelingWorksheetHybridError("confirmatory_eligible_work_count must be >= 100")
    thresholds = payload.get("threshold_check")
    if not isinstance(thresholds, Mapping):
        raise MLFreshEvalLabelingWorksheetHybridError("fresh surface threshold_check missing")
    label_keys = (
        "minimum_confirmatory_labeled_work_count",
        "minimum_confirmatory_label_coverage_rate",
        "minimum_confirmatory_negative_work_count",
        "minimum_distinct_negative_work_count",
    )
    if all(_get(thresholds, f"{key}.passed") is True for key in label_keys):
        raise MLFreshEvalLabelingWorksheetHybridError("fresh surface label/negative thresholds already met")


def _validate_policy(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshEvalLabelingWorksheetHybridError("fresh surface policy artifact_type mismatch")
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshEvalLabelingWorksheetHybridError("policy_version must be ml-fresh-eval-surface-policy-hybrid-v1")


def _label_rows_by_work(payload: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshEvalLabelingWorksheetHybridError("label dataset_version must be ml-label-dataset-v8")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLFreshEvalLabelingWorksheetHybridError("label dataset missing rows array")
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if not row_has_explicit_label({str(k): "" if v is None else str(v) for k, v in row.items()}):
            continue
        canonical = _canonical_work_id_from_label(row)
        if not canonical:
            continue
        out.setdefault(canonical, []).append(dict(row))
    return out


def _float_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_value(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _abstract_preview(value: Any, *, max_chars: int = 360) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _format_float(value: Any) -> str:
    number = _float_value(value)
    if number is None:
        return ""
    return f"{number:.6f}".rstrip("0").rstrip(".")


def _fetch_db_context(
    *,
    database_url: str | None,
    work_ids: Sequence[int],
) -> tuple[dict[int, dict[str, Any]], dict[str, Any] | None]:
    if not database_url:
        return {}, None
    db_summary = _assert_local_database_url(database_url)
    unique_ids = sorted({int(work_id) for work_id in work_ids if work_id is not None})
    if not unique_ids:
        return {}, db_summary
    sql = """
        SELECT id, openalex_id, title, abstract, source_slug, year, citation_count
        FROM works
        WHERE id = ANY(%s)
    """
    with psycopg.connect(database_url, autocommit=True) as conn:
        rows = conn.execute(sql, (unique_ids,)).fetchall()
    context: dict[int, dict[str, Any]] = {}
    for row in rows:
        context[int(row[0])] = {
            "internal_work_id": int(row[0]),
            "openalex_id": row[1],
            "title": row[2],
            "abstract": row[3],
            "source_slug": row[4],
            "year": row[5],
            "citation_count": row[6],
        }
    return context, db_summary


def _candidate_rows(surface_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _get(surface_payload, "candidate_pool.candidate_rows")
    if not isinstance(rows, list):
        raise MLFreshEvalLabelingWorksheetHybridError("fresh surface candidate_pool.candidate_rows missing")
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        canonical = _canonical_from_value(row.get("canonical_openalex_work_id") or row.get("openalex_id"))
        if canonical:
            normalized = dict(row)
            normalized["canonical_openalex_work_id"] = canonical
            out.append(normalized)
    return out


def _score_sort_value(row: Mapping[str, Any]) -> tuple[float, int, str]:
    score = _float_value(row.get("final_score"))
    rank = _int_value(row.get("heuristic_rank")) or 10**9
    return (score if score is not None else 1e9, rank, str(row.get("canonical_openalex_work_id") or ""))


def _rank_sort_value(row: Mapping[str, Any]) -> tuple[int, float, str]:
    rank = _int_value(row.get("heuristic_rank")) or 10**9
    score = _float_value(row.get("final_score"))
    return (rank, -(score if score is not None else -1e9), str(row.get("canonical_openalex_work_id") or ""))


def _add_from_sequence(
    selected: list[tuple[dict[str, Any], str]],
    seen: set[str],
    rows: Sequence[dict[str, Any]],
    *,
    limit: int,
    reason: str,
) -> None:
    for row in rows:
        if len(selected) >= limit:
            return
        canonical = str(row["canonical_openalex_work_id"])
        if canonical in seen:
            continue
        seen.add(canonical)
        selected.append((row, reason))


def _sample_candidates(
    candidates: Sequence[dict[str, Any]],
    *,
    requested_rows: int,
    seed: int,
) -> list[tuple[dict[str, Any], str]]:
    target = min(max(int(requested_rows), 0), len(candidates))
    if target <= 0:
        return []
    by_low_score = sorted(candidates, key=_score_sort_value)
    selected: list[tuple[dict[str, Any], str]] = []
    seen: set[str] = set()

    negative_target = min(len(candidates), max(1, math.ceil(target * 0.35)))
    _add_from_sequence(
        selected,
        seen,
        by_low_score,
        limit=min(target, negative_target),
        reason="fresh_hybrid_negative_candidate",
    )

    boundary_target = min(target, len(selected) + max(1, math.ceil(target * 0.25)))
    low_mid_start = max(0, math.floor(len(by_low_score) * 0.20))
    low_mid_end = max(low_mid_start + 1, math.ceil(len(by_low_score) * 0.60))
    _add_from_sequence(
        selected,
        seen,
        by_low_score[low_mid_start:low_mid_end],
        limit=boundary_target,
        reason="fresh_hybrid_score_boundary",
    )

    spread_target = min(target, len(selected) + max(1, math.ceil(target * 0.25)))
    by_rank = sorted(candidates, key=_rank_sort_value)
    if by_rank:
        if spread_target > len(selected):
            needed = spread_target - len(selected)
            indexes = [
                round(i * (len(by_rank) - 1) / max(needed - 1, 1))
                for i in range(needed)
            ]
            spread_rows = [by_rank[index] for index in indexes]
        else:
            spread_rows = []
        _add_from_sequence(
            selected,
            seen,
            spread_rows,
            limit=spread_target,
            reason="fresh_hybrid_score_spread",
        )

    if len(selected) < target:
        remaining = [row for row in candidates if str(row["canonical_openalex_work_id"]) not in seen]
        random.Random(seed).shuffle(remaining)
        _add_from_sequence(
            selected,
            seen,
            remaining,
            limit=target,
            reason="fresh_hybrid_seeded_fill",
        )
    return selected


def _threshold_gap_before_labeling(surface_payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    thresholds = surface_payload.get("threshold_check")
    if not isinstance(thresholds, Mapping):
        return {}
    gaps: dict[str, dict[str, Any]] = {}
    for key, row in thresholds.items():
        if not isinstance(row, Mapping):
            continue
        observed = row.get("observed")
        threshold = row.get("threshold")
        deficit: int | float
        if key == "minimum_confirmatory_label_coverage_rate":
            eligible = int(_get(surface_payload, "confirmatory_eligibility.confirmatory_metric_eligible_work_count") or 0)
            labeled = int(_get(surface_payload, "label_coverage.work_level.confirmatory_labeled_work_count") or 0)
            needed = math.ceil(float(threshold or 0) * eligible)
            deficit = max(0, needed - labeled)
            note = f"coverage deficit in labeled works: ceil({threshold} * {eligible}) - {labeled}"
        else:
            deficit = max(0, (threshold or 0) - (observed or 0)) if isinstance(threshold, (int, float)) and isinstance(observed, (int, float)) else 0
            note = None
        gaps[str(key)] = {
            "observed": observed,
            "threshold": threshold,
            "deficit": deficit,
            "passed": bool(row.get("passed")),
            "notes": note,
        }
    return gaps


def _existing_label_summary(labels: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in labels:
        out.append(
            {
                "row_id": row.get("row_id"),
                "good_or_acceptable": row.get(TARGET),
                "relevance_label": row.get("relevance_label"),
                "novelty_label": row.get("novelty_label"),
            }
        )
    return out


def _worksheet_row(
    row: Mapping[str, Any],
    *,
    db_context: Mapping[str, Any] | None,
    row_id: str,
    worksheet_version: str,
    review_pool_variant: str,
    sample_reason: str,
) -> dict[str, str]:
    context = dict(db_context or {})
    canonical = str(row["canonical_openalex_work_id"])
    openalex_id = str(row.get("openalex_id") or context.get("openalex_id") or f"https://openalex.org/{canonical}")
    title = str(row.get("title") or context.get("title") or "")
    year = row.get("year") if row.get("year") is not None else context.get("year")
    citation_count = row.get("citation_count") if row.get("citation_count") is not None else context.get("citation_count")
    return {
        "row_id": row_id,
        "worksheet_version": worksheet_version,
        "review_pool_variant": review_pool_variant,
        "paper_id": openalex_id,
        "openalex_work_id": canonical,
        "work_id": canonical,
        "title": title,
        "year": "" if year is None else str(year),
        "citation_count": "" if citation_count is None else str(citation_count),
        "source_slug": str(row.get("source_slug") or context.get("source_slug") or ""),
        "topics": str(row.get("topics") or context.get("topics") or ""),
        "abstract_preview": _abstract_preview(row.get("abstract") or context.get("abstract")),
        "sample_reason": sample_reason,
        "ranking_run_id": str(row.get("ranking_run_id") or ""),
        "family": str(row.get("family") or ""),
        "final_score": _format_float(row.get("final_score")),
        "rank_in_family": "" if row.get("heuristic_rank") is None else str(row.get("heuristic_rank")),
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def render_worksheet_csv(rows: Sequence[Mapping[str, str]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(CSV_COLUMNS), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in CSV_COLUMNS})
    return out.getvalue()


def build_ml_fresh_eval_labeling_worksheet_hybrid_payloads(
    *,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    rows: int = DEFAULT_ROWS,
    seed: int = DEFAULT_SEED,
    worksheet_version: str = WORKSHEET_VERSION,
    review_pool_variant: str = DEFAULT_REVIEW_POOL_VARIANT,
    database_url: str | None = None,
    repo_root: Path | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any], str]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    surface_path = Path(fresh_eval_surface_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    labels_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()
    surface_payload = _load_json_object(surface_path)
    policy_payload = _load_json_object(policy_path)
    label_payload = _load_json_object(labels_path)
    _validate_surface(surface_payload)
    _validate_policy(policy_payload)
    label_groups = _label_rows_by_work(label_payload)
    if not conflict_path.exists():
        raise MLFreshEvalLabelingWorksheetHybridError(f"conflict policy does not exist: {conflict_path}")
    if rows < 0:
        raise MLFreshEvalLabelingWorksheetHybridError("--rows must be non-negative")
    if not str(review_pool_variant or "").strip():
        raise MLFreshEvalLabelingWorksheetHybridError("review_pool_variant must not be blank")

    candidates = _candidate_rows(surface_payload)
    eligible = [
        row
        for row in candidates
        if row.get("confirmatory_metric_eligible") is True
        and row.get("previous_eval_overlap") is not True
        and str(row["canonical_openalex_work_id"]) not in label_groups
    ]
    eligible_by_work: dict[str, dict[str, Any]] = {}
    for row in eligible:
        eligible_by_work.setdefault(str(row["canonical_openalex_work_id"]), row)
    available = list(eligible_by_work.values())
    requested = int(rows)
    selected = _sample_candidates(available, requested_rows=requested, seed=int(seed))

    db_context: dict[int, dict[str, Any]] = {}
    database_summary: dict[str, Any] | None = None
    if database_url:
        work_ids = [int(row["internal_work_id"]) for row, _reason in selected if row.get("internal_work_id") is not None]
        db_context, database_summary = _fetch_db_context(database_url=database_url, work_ids=work_ids)

    worksheet_rows: list[dict[str, str]] = []
    sidecar_rows: list[dict[str, Any]] = []
    for row, sample_reason in selected:
        canonical = str(row["canonical_openalex_work_id"])
        row_id = stable_row_id(
            worksheet_version=worksheet_version,
            seed=int(seed),
            canonical_openalex_work_id=canonical,
        )
        context = db_context.get(int(row["internal_work_id"])) if row.get("internal_work_id") is not None else None
        csv_row = _worksheet_row(
            row,
            db_context=context,
            row_id=row_id,
            worksheet_version=worksheet_version,
            review_pool_variant=str(review_pool_variant),
            sample_reason=sample_reason,
        )
        worksheet_rows.append(csv_row)
        sidecar_rows.append(
            {
                "row_id": row_id,
                "canonical_openalex_work_id": canonical,
                "paper_id": csv_row["paper_id"],
                "ranking_run_id": csv_row["ranking_run_id"],
                "family": csv_row["family"],
                "rank_in_family": csv_row["rank_in_family"],
                "final_score": row.get("final_score"),
                "semantic_score": row.get("semantic_score"),
                "citation_velocity_score": row.get("citation_velocity_score"),
                "topic_growth_score": row.get("topic_growth_score"),
                "bridge_score": row.get("bridge_score"),
                "bridge_eligible": row.get("bridge_eligible"),
                "sample_reason": sample_reason,
                "title": csv_row["title"],
                "year": csv_row["year"],
                "citation_count": csv_row["citation_count"],
                "source_slug": csv_row["source_slug"],
                "topics": csv_row["topics"],
                "existing_v8_labels_for_same_work": _existing_label_summary(label_groups.get(canonical, [])),
                "split_intent": "future_confirmatory_eval_only",
            }
        )

    inputs = [
        _input_record("fresh_eval_surface", surface_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", labels_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    source_summary = {
        "ranking_run_id": _get(surface_payload, "candidate_source.ranking_run_id"),
        "family": _get(surface_payload, "candidate_source.family"),
        "snapshot_version": _get(surface_payload, "candidate_source.corpus_snapshot_version"),
        "candidate_work_count": _get(surface_payload, "candidate_pool.candidate_work_count"),
        "confirmatory_eligible_work_count": _get(
            surface_payload,
            "confirmatory_eligibility.confirmatory_metric_eligible_work_count",
        ),
        "candidate_work_set_sha256": _get(surface_payload, "candidate_pool.candidate_work_set_sha256"),
        "old_eval_overlap_excluded_count": _get(
            surface_payload,
            "disallowed_overlap_report.excluded_previous_eval_overlap_count",
        ),
        "label_coverage_rate": _get(surface_payload, "label_coverage.work_level.label_coverage_rate"),
        "labeled_work_count": _get(surface_payload, "label_coverage.work_level.confirmatory_labeled_work_count"),
        "positive_work_count": _get(surface_payload, "label_coverage.work_level.confirmatory_positive_work_count"),
        "negative_work_count": _get(surface_payload, "label_coverage.work_level.confirmatory_negative_work_count"),
        "distinct_negative_work_count": _get(surface_payload, "label_coverage.work_level.distinct_negative_work_count"),
    }
    reason_counts = dict(Counter(row["sample_reason"] for row in worksheet_rows))
    context_payload = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "worksheet_version": worksheet_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "seed": int(seed),
            "requested_rows": requested,
            "achieved_rows": len(worksheet_rows),
            "available_unlabeled_confirmatory_eligible_work_count": len(available),
            "shortfall_count": max(0, requested - len(worksheet_rows)),
            "review_pool_variant": str(review_pool_variant),
            "database_target_redacted": database_summary.get("database_target_redacted") if database_summary else None,
            "database_access": "read_only_select" if database_url else "not_used_artifact_fields_sufficient",
            "source_surface_summary": source_summary,
            "threshold_gap_before_labeling": _threshold_gap_before_labeling(surface_payload),
            "label_sets": ALLOWED_LABEL_SETS,
            "caveats": list(CAVEATS),
        },
        "sampling_strategy": {
            "deterministic_seed": int(seed),
            "exclude_old_217_overlap": True,
            "exclude_existing_v8_labeled_works": True,
            "sample_reason_breakdown": reason_counts,
            "sample_reason_definitions": {
                "fresh_hybrid_negative_candidate": "Lower final_score fresh candidates prioritized for negative/borderline label discovery.",
                "fresh_hybrid_score_boundary": "Low-to-mid score boundary candidates included for classification boundary coverage.",
                "fresh_hybrid_score_spread": "Deterministic rank/score spread across the full eligible pool.",
                "fresh_hybrid_seeded_fill": "Seeded deterministic fill after priority buckets.",
            },
        },
        "row_id_policy": {
            "formula": "sha256(f\"{worksheet_version}|{seed}|{canonical_openalex_work_id}\")",
            "csv_row_id_set_equals_sidecar_row_id_set": True,
        },
        "rows": sidecar_rows,
        "blocked_actions": [
            "label_ingest",
            "model_training",
            "scoring",
            "ranking",
            "embeddings",
            "hybrid_validation",
            "shadow",
            "production_default",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
    }
    markdown = markdown_from_context(context_payload)
    return worksheet_rows, context_payload, markdown


def markdown_from_context(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    source = metadata["source_surface_summary"]
    gaps = metadata["threshold_gap_before_labeling"]
    reason_counts = payload["sampling_strategy"]["sample_reason_breakdown"]
    lines = [
        f"# Fresh Hybrid Eval Labeling Worksheet ({metadata['worksheet_version']})",
        "",
        "## Summary",
        "",
        "This worksheet is for manual labels on fresh confirmatory-eligible hybrid eval works. The CSV label fields are blank; this command does not ingest labels or run validation.",
        "",
        f"- Ranking run: `{source['ranking_run_id']}`",
        f"- Family: `{source['family']}`",
        f"- Snapshot: `{source['snapshot_version']}`",
        f"- Confirmatory eligible works: {source['confirmatory_eligible_work_count']}",
        f"- Existing labeled works: {source['labeled_work_count']}",
        f"- Requested / achieved worksheet rows: {metadata['requested_rows']} / {metadata['achieved_rows']}",
        f"- Shortfall: {metadata['shortfall_count']}",
        "",
        "## Threshold Gaps Before Labeling",
        "",
        "| Threshold | Observed | Required | Deficit | Passed |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for key, row in gaps.items():
        lines.append(
            f"| `{key}` | {row.get('observed')} | {row.get('threshold')} | {row.get('deficit')} | {row.get('passed')} |"
        )
    lines.extend(
        [
            "",
            "## Sampling Strategy",
            "",
        ]
    )
    lines.extend(f"- {reason}: {count}" for reason, count in sorted(reason_counts.items()))
    lines.extend(
        [
            "",
            "## Rubric",
            "",
            "- relevance_label: good, acceptable, miss, irrelevant",
            "- novelty_label: surprising, useful, obvious, not_useful, neither",
            "- bridge_like_label: yes, partial, no, not_applicable",
            "- reviewer_notes: free text",
            "",
            "## Worksheet Only / Not Validation / No Shadow-Prod",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.extend(
        [
            "",
            "## Next Step",
            "",
            "Human reviewer fills the CSV, then a future explicit label ingest creates a v9 or fresh-hybrid label ingest artifact. Hybrid validation remains blocked until materialization shows policy thresholds pass.",
            "",
        ]
    )
    return "\n".join(lines)


def write_ml_fresh_eval_labeling_worksheet_hybrid(
    *,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    rows: int = DEFAULT_ROWS,
    seed: int = DEFAULT_SEED,
    worksheet_version: str = WORKSHEET_VERSION,
    review_pool_variant: str = DEFAULT_REVIEW_POOL_VARIANT,
    database_url: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    worksheet_rows, context_payload, markdown = build_ml_fresh_eval_labeling_worksheet_hybrid_payloads(
        fresh_eval_surface_path=fresh_eval_surface_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        rows=rows,
        seed=seed,
        worksheet_version=worksheet_version,
        review_pool_variant=review_pool_variant,
        database_url=database_url,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_worksheet_csv(worksheet_rows), encoding="utf-8", newline="")
    context_output_path.parent.mkdir(parents=True, exist_ok=True)
    context_output_path.write_text(json.dumps(context_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown, encoding="utf-8", newline="\n")
    return context_payload


__all__ = [
    "ARTIFACT_TYPE",
    "WORKSHEET_VERSION",
    "MLFreshEvalLabelingWorksheetHybridError",
    "build_ml_fresh_eval_labeling_worksheet_hybrid_payloads",
    "render_worksheet_csv",
    "stable_row_id",
    "write_ml_fresh_eval_labeling_worksheet_hybrid",
]
