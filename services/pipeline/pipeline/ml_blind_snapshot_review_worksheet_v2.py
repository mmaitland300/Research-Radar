"""Reviewer-blind snapshot worksheet v2 with hidden ranking context sidecar."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_blind_snapshot_review_worksheet import (
    ABSTRACT_PREVIEW_MAX_CHARS,
    ALLOWED_SAMPLE_REASONS,
    MAX_ROWS,
    MIN_ROWS,
    MLBlindSnapshotReviewWorksheetError,
    REVIEW_POOL_VARIANT,
    BlindCandidate,
    _citation_band,
    _truncate_abstract,
    _year_band,
    assert_succeeded_clustering_run,
    assert_succeeded_ranking_run,
    fetch_candidate_pool,
    fetch_ranking_context,
    fully_labeled_work_tokens,
    load_label_dataset_payload,
    raw_pool_to_candidates,
    select_blind_sample,
)
from pipeline.repo_paths import portable_repo_path

WORKSHEET_VERSION_V2 = "ml-blind-snapshot-review-v2"
DEFAULT_V2_SEED = 20260512

V2_CSV_COLUMNS: tuple[str, ...] = (
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

V2_CAVEATS: tuple[str, ...] = (
    "This worksheet is for reviewer-blind manual labeling, not validation.",
    "The reviewer CSV intentionally hides ranking scores, family ranks, learned logits, and model predictions.",
    "Selection strata may use ranking DB fields off-worksheet for sampling diversity only; the sidecar preserves that provenance.",
    "Rows are audit-only until a deliberate train/dev/test policy exists.",
    "No model is trained and no ranking is run by this worksheet command.",
)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_row_id(*, worksheet_version: str, sample_seed: int, paper_id: str) -> str:
    raw = f"{worksheet_version}|{sample_seed}|{paper_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def fetch_paper_score_feature_context(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
) -> dict[int, dict[str, dict[str, Any]]]:
    q = """
        SELECT
            ps.work_id,
            ps.recommendation_family,
            ps.final_score,
            ps.semantic_score,
            ps.citation_velocity_score,
            ps.topic_growth_score,
            ps.diversity_penalty,
            ps.bridge_score
        FROM paper_scores ps
        WHERE ps.ranking_run_id = %s
        ORDER BY ps.work_id ASC, ps.recommendation_family ASC
    """
    rows = list(conn.execute(q, (ranking_run_id,)).fetchall())
    out: dict[int, dict[str, dict[str, Any]]] = {}
    for r in rows:
        wid = int(r["work_id"])
        fam = str(r["recommendation_family"])
        out.setdefault(wid, {})[fam] = {
            "family": fam,
            "final_score": float(r["final_score"]) if r["final_score"] is not None else None,
            "semantic_score": float(r["semantic_score"]) if r["semantic_score"] is not None else None,
            "citation_velocity_score": (
                float(r["citation_velocity_score"]) if r["citation_velocity_score"] is not None else None
            ),
            "topic_growth_score": float(r["topic_growth_score"]) if r["topic_growth_score"] is not None else None,
            "diversity_penalty": float(r["diversity_penalty"]) if r["diversity_penalty"] is not None else None,
            "bridge_score": float(r["bridge_score"]) if r["bridge_score"] is not None else None,
        }
    return out


def _family_json(data: dict[str, Any]) -> str:
    if not data:
        return ""
    return json.dumps({k: data[k] for k in sorted(data)}, sort_keys=True)


def _candidate_csv_row(
    *,
    cand: BlindCandidate,
    sample_reason: str,
    seed: int,
) -> dict[str, str]:
    rid = stable_row_id(worksheet_version=WORKSHEET_VERSION_V2, sample_seed=seed, paper_id=cand.paper_id)
    return {
        "row_id": rid,
        "worksheet_version": WORKSHEET_VERSION_V2,
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
        "sample_reason": sample_reason,
        "cluster_id": cand.cluster_id,
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def _candidate_sidecar_row(
    *,
    cand: BlindCandidate,
    sample_reason: str,
    seed: int,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
    score_feature_context: dict[int, dict[str, dict[str, Any]]],
) -> dict[str, Any]:
    rid = stable_row_id(worksheet_version=WORKSHEET_VERSION_V2, sample_seed=seed, paper_id=cand.paper_id)
    family_features = score_feature_context.get(cand.internal_work_id, {})
    emerging_features = family_features.get("emerging")
    return {
        "row_id": rid,
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "internal_work_id": cand.internal_work_id,
        "sample_seed": seed,
        "sample_reason": sample_reason,
        "cluster_id": cand.cluster_id,
        "corpus_snapshot_version": corpus_snapshot_version,
        "embedding_version": embedding_version,
        "cluster_version": cluster_version,
        "ranking_run_id": ranking_run_id,
        "ranking_context_family_scores_json": _family_json(cand.family_scores),
        "ranking_context_family_ranks_json": _family_json(cand.family_ranks),
        "emerging_paper_scores": emerging_features,
        "paper_scores_by_family": {fam: family_features[fam] for fam in sorted(family_features)},
    }


def render_v2_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=V2_CSV_COLUMNS, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in V2_CSV_COLUMNS})
    return buf.getvalue()


def build_v2_context_payload(
    *,
    sidecar_rows: Sequence[dict[str, Any]],
    label_dataset_path: Path,
    label_dataset_sha256: str,
    requested_rows: int,
    debug: dict[str, Any],
    seed: int,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
) -> dict[str, Any]:
    return {
        "artifact_type": "ml_blind_snapshot_review_v2_context",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "provenance": {
            "worksheet_version": WORKSHEET_VERSION_V2,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "sample_seed": seed,
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "label_dataset_path": portable_repo_path(label_dataset_path),
            "label_dataset_sha256": label_dataset_sha256,
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
                "Reviewer CSV stores OpenAlex W tokens in work_id. Postgres works.id appears only as "
                "internal_work_id in this sidecar."
            ),
        },
        "sampling_debug": dict(debug),
        "rows": list(sidecar_rows),
    }


def render_v2_markdown(
    *,
    selected: Sequence[tuple[BlindCandidate, str]],
    debug: dict[str, Any],
    seed: int,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
    label_dataset_path: Path,
    label_dataset_sha256: str,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    requested_rows: int,
) -> str:
    by_reason: Counter[str] = Counter(reason for _cand, reason in selected)
    by_cluster: Counter[str] = Counter(cand.cluster_id for cand, _reason in selected)
    by_year_band: Counter[str] = Counter(_year_band(cand.year) for cand, _reason in selected)
    by_cite_band: Counter[str] = Counter(_citation_band(cand.citation_count) for cand, _reason in selected)
    achieved = int(debug.get("achieved_rows", len(selected)))
    lines = [
        f"# Blind snapshot review worksheet (`{WORKSHEET_VERSION_V2}`)",
        "",
        "## Purpose",
        "",
        "Second reviewer-blind snapshot worksheet for cleaner manual labels after weak blind transfer diagnostics. "
        "The CSV hides ranking scores, ranks, family score JSON, learned logits, and model predictions; those fields live only in the sidecar JSON.",
        "",
        "## Provenance",
        "",
        f"- **worksheet_version:** `{WORKSHEET_VERSION_V2}`",
        f"- **review_pool_variant:** `{REVIEW_POOL_VARIANT}`",
        f"- **sample_seed:** `{seed}`",
        f"- **row_id formula:** `sha256(worksheet_version|sample_seed|paper_id)`",
        f"- **label_dataset:** `{portable_repo_path(label_dataset_path)}`",
        f"- **label_dataset_sha256:** `{label_dataset_sha256}`",
        f"- **ranking_run_id:** `{ranking_run_id}`",
        f"- **corpus_snapshot_version:** `{corpus_snapshot_version}`",
        f"- **embedding_version:** `{embedding_version}`",
        f"- **cluster_version:** `{cluster_version}`",
        f"- **csv_output:** `{portable_repo_path(csv_output_path)}`",
        f"- **context_sidecar_output:** `{portable_repo_path(context_output_path)}`",
        f"- **markdown_output:** `{portable_repo_path(markdown_output_path)}`",
        "",
        "## Reviewer Blindness",
        "",
        "The reviewer CSV excludes `ranking_run_id`, internal Postgres IDs, `final_score`, score components, family score/rank JSON, learned logits, and model predictions. "
        "Selection strata may use ranking DB fields off-worksheet for sampling diversity only; the worksheet itself is not sorted by `final_score` or any ranking metric.",
        "",
        "## Sidecar Schema",
        "",
        "The JSON sidecar is keyed by `row_id` for merge with the CSV and contains `internal_work_id`, `ranking_run_id`, family score/rank JSON, and persisted `paper_scores` features.",
        "",
        "## Sample Summary",
        "",
        f"- **requested rows:** `{requested_rows}`",
        f"- **achieved rows:** `{achieved}`",
        f"- **eligible unlabeled pool size:** `{debug.get('eligible_pool_size')}`",
        f"- **excluded as already fully labeled:** `{debug.get('fully_labeled_excluded_count')}`",
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
        "## Caveats",
        "",
        *[f"- {c}" for c in V2_CAVEATS],
        "",
    ]
    return "\n".join(lines)


def build_blind_snapshot_review_worksheet_v2(
    *,
    database_url: str | None,
    label_dataset_path: Path,
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
        raise MLBlindSnapshotReviewWorksheetError(f"--rows must be between {MIN_ROWS} and {MAX_ROWS}")
    if not label_dataset_path.is_file():
        raise MLBlindSnapshotReviewWorksheetError(f"label dataset not found: {label_dataset_path}")
    payload = load_label_dataset_payload(label_dataset_path)
    label_sha = hashlib.sha256(label_dataset_path.read_bytes()).hexdigest()
    fully_labeled = fully_labeled_work_tokens(payload)

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
    selected, debug = select_blind_sample(candidates, fully_labeled_tokens=fully_labeled, total_rows=rows, seed=seed)

    csv_rows = [_candidate_csv_row(cand=cand, sample_reason=reason, seed=seed) for cand, reason in selected]
    sidecar_rows = [
        _candidate_sidecar_row(
            cand=cand,
            sample_reason=reason,
            seed=seed,
            corpus_snapshot_version=corpus_snapshot_version,
            embedding_version=embedding_version,
            cluster_version=cluster_version,
            ranking_run_id=ranking_run_id,
            score_feature_context=score_feature_context,
        )
        for cand, reason in selected
    ]
    csv_text = render_v2_csv(csv_rows)
    context_payload = build_v2_context_payload(
        sidecar_rows=sidecar_rows,
        label_dataset_path=label_dataset_path,
        label_dataset_sha256=label_sha,
        requested_rows=rows,
        debug=debug,
        seed=seed,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        cluster_version=cluster_version,
        ranking_run_id=ranking_run_id,
    )
    md_text = render_v2_markdown(
        selected=selected,
        debug=debug,
        seed=seed,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        cluster_version=cluster_version,
        ranking_run_id=ranking_run_id,
        label_dataset_path=label_dataset_path,
        label_dataset_sha256=label_sha,
        csv_output_path=csv_output_path,
        context_output_path=context_output_path,
        markdown_output_path=markdown_output_path,
        requested_rows=rows,
    )
    return csv_text, context_payload, md_text, debug


def run_ml_blind_snapshot_review_worksheet_v2_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
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
    csv_text, context_payload, md_text, debug = build_blind_snapshot_review_worksheet_v2(
        database_url=database_url,
        label_dataset_path=label_dataset_path,
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
    "DEFAULT_V2_SEED",
    "HIDDEN_REVIEWER_CSV_FIELDS",
    "V2_CAVEATS",
    "V2_CSV_COLUMNS",
    "WORKSHEET_VERSION_V2",
    "build_blind_snapshot_review_worksheet_v2",
    "build_v2_context_payload",
    "fetch_paper_score_feature_context",
    "render_v2_csv",
    "render_v2_markdown",
    "run_ml_blind_snapshot_review_worksheet_v2_cli",
    "stable_row_id",
]
