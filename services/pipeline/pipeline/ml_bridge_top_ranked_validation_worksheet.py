"""Bridge top-ranked validation worksheet generator.

Produces three outputs:
  1. Reviewer CSV: top-ranked bridge papers + contrastive borderline rows.
  2. Context sidecar JSON: hidden scores, bridge_score coverage report, and provenance.
  3. Markdown summary: coverage findings and sample stats.

This is label-collection only. No model is trained or served. All DB access is
SELECT-only, enforced by _execute_select.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_bridge_negative_mining_worksheet import (
    ABSTRACT_PREVIEW_MAX_CHARS,
    BridgeMiningCandidate,
    assert_succeeded_ranking_run,
    build_bridge_exclusion_sets,
    fetch_bridge_scored_rows,
    raw_row_to_candidate,
    stable_row_id as _base_stable_row_id,
)
from pipeline.ml_label_dataset import paper_id_to_work_id, sha256_file
from pipeline.ml_offline_baseline_eval import load_label_dataset as load_label_dataset_json
from pipeline.ml_contrastive_review_worksheet import _parse_config_json
from pipeline.recommendation_review_worksheet import (
    _topic_names_from_json,
    cluster_version_from_config,
)
from pipeline.repo_paths import portable_repo_path

WORKSHEET_VERSION = "ml-bridge-top-ranked-v1"
REVIEW_POOL_VARIANT = "ml_bridge_top_ranked_validation_audit"
DEFAULT_RANKING_RUN_ID = "rank-83787b91ef"
DEFAULT_SAMPLE_SEED = 20260601
DEFAULT_TOP_N = 20
DEFAULT_CONTRASTIVE_N = 10
DEFAULT_CONTRASTIVE_RANK_MAX = 40

WRITE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|merge|grant|revoke|vacuum|reindex|copy)\b"
)

ALLOWED_SAMPLE_REASONS: tuple[str, ...] = (
    "bridge_top_ranked",
    "bridge_borderline_contrastive",
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
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)

HIDDEN_REVIEWER_CSV_FIELDS: tuple[str, ...] = (
    "ranking_run_id",
    "ranking_version",
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
    "family",
    "family_rank",
    "internal_work_id",
    "final_score",
    "semantic_score",
    "citation_velocity_score",
    "topic_growth_score",
    "bridge_score",
    "diversity_penalty",
    "bridge_eligible",
    "reason_short",
    "bridge_score_coverage_note",
)

CAVEATS: tuple[str, ...] = (
    "This worksheet is for offline label collection only.",
    "Top-ranked rows surface what is currently live on the Bridge feed.",
    "Borderline-contrastive rows are just below the top-20 cut and not previously labeled.",
    "bridge_score is 0/528 non-null for rank-83787b91ef; ranking in this run is final_score-only.",
    "No bridge model is trained, no ranking is modified, and no production change is implied.",
)


class MLBridgeTopRankedValidationWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _execute_select(cur: Any, sql: str, params: Sequence[Any] | None = None) -> Any:
    stripped = sql.strip()
    lowered = stripped.lower()
    if not lowered.startswith("select"):
        raise MLBridgeTopRankedValidationWorksheetError(
            "DB safety violation: SQL must start with SELECT"
        )
    if WRITE_SQL_RE.search(lowered):
        raise MLBridgeTopRankedValidationWorksheetError(
            "DB safety violation: SQL contains write/DDL verb"
        )
    return cur.execute(sql, tuple(params or ()))


def stable_row_id(*, paper_id: str) -> str:
    raw = f"{WORKSHEET_VERSION}|{DEFAULT_SAMPLE_SEED}|{paper_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _truncate_abstract(abstract: str) -> str:
    text = " ".join(abstract.split())
    if len(text) <= ABSTRACT_PREVIEW_MAX_CHARS:
        return text
    return text[: ABSTRACT_PREVIEW_MAX_CHARS - 1].rstrip() + "\u2026"


def fetch_bridge_score_coverage(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
) -> dict[str, Any]:
    """Query bridge_score population stats for a bridge ranking run, per rank band."""
    sql = """
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN bridge_score IS NULL THEN 1 ELSE 0 END) AS null_count,
            SUM(CASE WHEN bridge_score = 0 THEN 1 ELSE 0 END) AS zero_count,
            SUM(CASE WHEN bridge_score IS NOT NULL AND bridge_score != 0
                     THEN 1 ELSE 0 END) AS nonzero_count,
            MIN(CASE WHEN bridge_score IS NOT NULL AND bridge_score != 0
                     THEN bridge_score END) AS nonzero_min,
            MAX(bridge_score) AS nonzero_max
        FROM paper_scores
        WHERE ranking_run_id = %s
          AND recommendation_family = 'bridge'
    """
    with conn.cursor() as cur:
        _execute_select(cur, sql, [ranking_run_id])
        row = cur.fetchone()

    agg = dict(row) if row else {}
    total = int(agg.get("total_rows") or 0)
    null_c = int(agg.get("null_count") or 0)
    zero_c = int(agg.get("zero_count") or 0)
    nonzero_c = int(agg.get("nonzero_count") or 0)

    pct_covered = round(nonzero_c / total * 100, 1) if total > 0 else 0.0
    all_null = null_c == total and total > 0

    return {
        "ranking_run_id": ranking_run_id,
        "recommendation_family": "bridge",
        "total_rows": total,
        "null_count": null_c,
        "zero_count": zero_c,
        "nonzero_count": nonzero_c,
        "pct_covered": pct_covered,
        "nonzero_min": agg.get("nonzero_min"),
        "nonzero_max": agg.get("nonzero_max"),
        "all_null": all_null,
        "finding": (
            f"bridge_score is NULL for all {total} bridge rows in {ranking_run_id}. "
            "Bridge ranking is driven entirely by final_score on this run. "
            "A direct bridge_score + ML hybrid comparison is not possible until "
            "a run with populated bridge_score is available."
        ) if all_null else (
            f"bridge_score is populated for {nonzero_c}/{total} rows ({pct_covered}%) "
            f"in {ranking_run_id}."
        ),
    }


@dataclass(frozen=True)
class TopRankedSelection:
    candidate: BridgeMiningCandidate
    sample_reason: str


def select_top_ranked_sample(
    candidates: Sequence[BridgeMiningCandidate],
    *,
    already_labeled_this_run_ids: set[str],
    top_n: int = DEFAULT_TOP_N,
    contrastive_n: int = DEFAULT_CONTRASTIVE_N,
    contrastive_rank_max: int = DEFAULT_CONTRASTIVE_RANK_MAX,
) -> tuple[list[TopRankedSelection], dict[str, Any]]:
    """Select top-ranked rows + borderline contrastive rows.

    Top-ranked: family_rank 1..top_n (forced-include; all are unlabeled on this run).
    Contrastive: family_rank top_n+1..contrastive_rank_max, excluding papers already
    fully labeled for bridge on THIS ranking run (not cross-run exclusion).
    Both strata are returned in family_rank ascending order.
    """
    by_rank = {c.family_rank: c for c in candidates}

    top_rows = [
        TopRankedSelection(by_rank[r], "bridge_top_ranked")
        for r in sorted(by_rank)
        if r <= top_n and r in by_rank
    ]

    contrastive_pool = [
        c
        for c in candidates
        if top_n < c.family_rank <= contrastive_rank_max
        and c.work_token not in already_labeled_this_run_ids
    ]
    contrastive_pool.sort(key=lambda c: (c.family_rank, c.work_token))
    contrastive_rows = [
        TopRankedSelection(c, "bridge_borderline_contrastive")
        for c in contrastive_pool[:contrastive_n]
    ]

    selected = sorted(
        top_rows + contrastive_rows,
        key=lambda s: s.candidate.family_rank,
    )

    reason_counts = Counter(s.sample_reason for s in selected)
    top_fs = [s.candidate.final_score for s in top_rows]
    debug: dict[str, Any] = {
        "top_n_requested": top_n,
        "top_n_achieved": len(top_rows),
        "contrastive_n_requested": contrastive_n,
        "contrastive_n_achieved": len(contrastive_rows),
        "contrastive_rank_window": f"{top_n + 1}-{contrastive_rank_max}",
        "contrastive_pool_available": len(contrastive_pool),
        "already_labeled_this_run_ids": len(already_labeled_this_run_ids),
        "total_rows": len(selected),
        "sample_reason_counts": dict(sorted(reason_counts.items())),
        "top_ranked_final_score_range": (
            [min(top_fs), max(top_fs)] if top_fs else []
        ),
    }
    return selected, debug


def _candidate_csv_row(*, selection: TopRankedSelection) -> dict[str, str]:
    cand = selection.candidate
    topics_list = _topic_names_from_json(cand.topics_raw)
    return {
        "row_id": stable_row_id(paper_id=cand.paper_id),
        "worksheet_version": WORKSHEET_VERSION,
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "work_id": cand.work_token,
        "title": cand.title,
        "year": str(int(cand.year)) if cand.year is not None else "",
        "citation_count": str(int(cand.citation_count)),
        "source_slug": cand.source_slug,
        "topics": ";".join(topics_list) if topics_list else "",
        "abstract_preview": _truncate_abstract(cand.abstract) if cand.abstract else "",
        "sample_reason": selection.sample_reason,
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def _candidate_sidecar_row(
    *,
    selection: TopRankedSelection,
    run: dict[str, Any],
    cluster_version: str,
) -> dict[str, Any]:
    cand = selection.candidate
    return {
        "row_id": stable_row_id(paper_id=cand.paper_id),
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "internal_work_id": cand.internal_work_id,
        "sample_reason": selection.sample_reason,
        "family": "bridge",
        "family_rank": cand.family_rank,
        "ranking_run_id": str(run["ranking_run_id"]),
        "ranking_version": str(run["ranking_version"]),
        "corpus_snapshot_version": str(run["corpus_snapshot_version"]),
        "embedding_version": str(run["embedding_version"]),
        "cluster_version": cluster_version,
        "final_score": cand.final_score,
        "semantic_score": cand.semantic_score,
        "citation_velocity_score": cand.citation_velocity_score,
        "topic_growth_score": cand.topic_growth_score,
        "bridge_score": cand.bridge_score,
        "bridge_score_coverage_note": "bridge_score is NULL for all rows in this ranking run",
        "diversity_penalty": cand.diversity_penalty,
        "bridge_eligible": cand.bridge_eligible,
        "reason_short": cand.reason_short,
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
    coverage_report: dict[str, Any],
    label_dataset_path: Path,
    label_dataset_sha256: str,
    debug: dict[str, Any],
    ranking_run_id: str,
) -> dict[str, Any]:
    return {
        "artifact_type": "ml_bridge_top_ranked_validation_v1_context",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "provenance": {
            "worksheet_version": WORKSHEET_VERSION,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "sample_seed": DEFAULT_SAMPLE_SEED,
            "row_id_formula": f"sha256({WORKSHEET_VERSION}|{DEFAULT_SAMPLE_SEED}|paper_id)",
            "label_dataset_path": portable_repo_path(label_dataset_path),
            "label_dataset_sha256": label_dataset_sha256,
            "ranking_run_id": ranking_run_id,
        },
        "bridge_score_coverage": coverage_report,
        "schema": {
            "key": "row_id",
            "hidden_from_reviewer_csv": list(HIDDEN_REVIEWER_CSV_FIELDS),
            "notes": (
                "Reviewer CSV exposes sample_reason (top-ranked vs borderline-contrastive). "
                "Family rank, all scores, and bridge_score coverage note appear only in this sidecar."
            ),
        },
        "sampling_policy": {
            "top_ranked_stratum": f"family_rank 1-{debug.get('top_n_requested', DEFAULT_TOP_N)}, ordered by final_score desc",
            "contrastive_stratum": (
                f"family_rank {debug.get('top_n_requested', DEFAULT_TOP_N) + 1}"
                f"-{DEFAULT_CONTRASTIVE_RANK_MAX}, "
                "excluding papers already fully labeled for bridge on THIS run (not cross-run), ascending rank order"
            ),
            "display_order": "ascending family_rank within each stratum",
        },
        "sampling_debug": dict(debug),
        "rows": list(sidecar_rows),
    }


def render_markdown(
    *,
    selected: Sequence[TopRankedSelection],
    coverage_report: dict[str, Any],
    debug: dict[str, Any],
    run: dict[str, Any],
    cluster_version: str,
    label_dataset_path: Path,
    csv_output_path: Path,
    context_output_path: Path,
) -> str:
    by_reason: Counter[str] = Counter(s.sample_reason for s in selected)

    lines = [
        f"# Bridge top-ranked validation worksheet (`{WORKSHEET_VERSION}`)",
        "",
        "## Purpose",
        "",
        "Label-collection worksheet targeting the actual top-ranked Bridge papers on the "
        f"`{run['ranking_run_id']}` run, plus a contrastive borderline slice just below the "
        "top-20 cut. Complements the negative-mining worksheet (rank 26-528) with the "
        "visible Bridge surface.",
        "",
        "## bridge_score Coverage Report",
        "",
        f"- **ranking_run_id:** `{coverage_report['ranking_run_id']}`",
        f"- **total bridge rows:** `{coverage_report['total_rows']}`",
        f"- **bridge_score null:** `{coverage_report['null_count']}`",
        f"- **bridge_score zero:** `{coverage_report['zero_count']}`",
        f"- **bridge_score nonzero:** `{coverage_report['nonzero_count']}`",
        f"- **coverage:** `{coverage_report['pct_covered']}%`",
        "",
        f"> **Finding:** {coverage_report['finding']}",
        "",
        "This means the previous bounded-hybrid evaluation could not test "
        "`bridge_score + ML hybrid` on this run. Any future hybrid comparison "
        "requires a run with populated `bridge_score`.",
        "",
        "## Provenance",
        "",
        f"- **worksheet_version:** `{WORKSHEET_VERSION}`",
        f"- **review_pool_variant:** `{REVIEW_POOL_VARIANT}`",
        f"- **sample_seed:** `{DEFAULT_SAMPLE_SEED}`",
        f"- **ranking_run_id:** `{run['ranking_run_id']}`",
        f"- **ranking_version:** `{run['ranking_version']}`",
        f"- **corpus_snapshot_version:** `{run['corpus_snapshot_version']}`",
        f"- **embedding_version:** `{run['embedding_version']}`",
        f"- **cluster_version:** `{cluster_version or '(none)' }`",
        f"- **label_dataset:** `{portable_repo_path(label_dataset_path)}`",
        f"- **csv_output:** `{portable_repo_path(csv_output_path)}`",
        f"- **context_sidecar_output:** `{portable_repo_path(context_output_path)}`",
        "",
        "## Sample Summary",
        "",
        f"- **total rows:** `{debug['total_rows']}`",
        f"- **top_ranked rows (rank 1-{debug['top_n_requested']}):** `{debug['top_n_achieved']}`",
        f"- **contrastive_borderline rows (rank {debug['top_n_requested'] + 1}-"
        f"{DEFAULT_CONTRASTIVE_RANK_MAX}):** `{debug['contrastive_n_achieved']}`",
        f"- **contrastive pool available:** `{debug['contrastive_pool_available']}`",
        f"- **already-labeled on this run excluded from contrastive:** `{debug['already_labeled_this_run_ids']}`",
        f"- **top-ranked final_score range:** `{debug['top_ranked_final_score_range']}`",
        "",
        "## Row Counts By Sample Reason",
        "",
        "| sample_reason | rows |",
        "|---|---:|",
        *[f"| `{reason}` | {by_reason[reason]} |" for reason in ALLOWED_SAMPLE_REASONS if by_reason[reason] > 0],
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in CAVEATS],
        "",
    ]
    return "\n".join(lines)


def build_top_ranked_validation_worksheet(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    ranking_run_id: str,
    top_n: int = DEFAULT_TOP_N,
    contrastive_n: int = DEFAULT_CONTRASTIVE_N,
    contrastive_rank_max: int = DEFAULT_CONTRASTIVE_RANK_MAX,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
) -> dict[str, Any]:
    if not label_dataset_path.is_file():
        raise MLBridgeTopRankedValidationWorksheetError(
            f"label dataset not found: {label_dataset_path}"
        )
    rid = str(ranking_run_id).strip()
    if not rid:
        raise MLBridgeTopRankedValidationWorksheetError(
            "--ranking-run-id is required and must not be blank"
        )

    payload = load_label_dataset_json(label_dataset_path)
    label_sha = sha256_file(label_dataset_path)

    complete_run, _incomplete_run, _complete_bridge_any = build_bridge_exclusion_sets(
        payload, ranking_run_id=rid
    )
    # complete_run contains (family, work_token) tuples from paper_key_family.
    # Only exclude work_tokens fully labeled for bridge on THIS specific run (not cross-run).
    already_labeled_this_run_tokens: set[str] = {
        wt
        for (fam, wt) in complete_run
        if fam == "bridge" and wt
    }

    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        run = assert_succeeded_ranking_run(conn, ranking_run_id=rid)
        cfg = _parse_config_json(run.get("config_json"))
        cluster_ver = cluster_version_from_config(cfg) or ""
        raw_rows = fetch_bridge_scored_rows(conn, ranking_run_id=rid)
        coverage_report = fetch_bridge_score_coverage(conn, ranking_run_id=rid)

    candidates: list[BridgeMiningCandidate] = []
    for raw in raw_rows:
        cand = raw_row_to_candidate(dict(raw))
        if cand is not None:
            candidates.append(cand)

    if not candidates:
        raise MLBridgeTopRankedValidationWorksheetError(
            f"no bridge candidates found for {rid!r}"
        )

    selected, debug = select_top_ranked_sample(
        candidates,
        already_labeled_this_run_ids=already_labeled_this_run_tokens,
        top_n=top_n,
        contrastive_n=contrastive_n,
        contrastive_rank_max=contrastive_rank_max,
    )

    if not selected:
        raise MLBridgeTopRankedValidationWorksheetError(
            "no rows selected for top-ranked worksheet"
        )

    csv_rows = [_candidate_csv_row(selection=s) for s in selected]
    sidecar_rows = [
        _candidate_sidecar_row(selection=s, run=run, cluster_version=cluster_ver)
        for s in selected
    ]
    csv_text = render_csv(csv_rows)
    context_payload = build_context_payload(
        sidecar_rows=sidecar_rows,
        coverage_report=coverage_report,
        label_dataset_path=label_dataset_path,
        label_dataset_sha256=label_sha,
        debug=debug,
        ranking_run_id=rid,
    )
    md_text = render_markdown(
        selected=selected,
        coverage_report=coverage_report,
        debug=debug,
        run=run,
        cluster_version=cluster_ver,
        label_dataset_path=label_dataset_path,
        csv_output_path=csv_output_path,
        context_output_path=context_output_path,
    )

    csv_output_path.parent.mkdir(parents=True, exist_ok=True)
    context_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)

    csv_output_path.write_text(csv_text, encoding="utf-8")
    context_output_path.write_text(
        json.dumps(context_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    markdown_output_path.write_text(md_text, encoding="utf-8")

    return {
        "coverage_report": coverage_report,
        "debug": debug,
        "csv_path": str(csv_output_path.resolve()),
        "context_path": str(context_output_path.resolve()),
        "markdown_path": str(markdown_output_path.resolve()),
    }


def run_top_ranked_validation_worksheet_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    ranking_run_id: str,
    top_n: int,
    contrastive_n: int,
    contrastive_rank_max: int,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
) -> dict[str, Any]:
    return build_top_ranked_validation_worksheet(
        database_url=database_url,
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
        top_n=top_n,
        contrastive_n=contrastive_n,
        contrastive_rank_max=contrastive_rank_max,
        csv_output_path=csv_output_path,
        context_output_path=context_output_path,
        markdown_output_path=markdown_output_path,
    )
