"""Reviewer-blind bridge negative-mining worksheet from a persisted bridge ranking run.

Read-only DB helper for collecting bridge-surface contrast labels, especially
likely rejects below the heuristic top band. Ranking scores stay in the sidecar.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_contrastive_review_worksheet import (
    build_label_key_sets,
    paper_key_family,
)
from pipeline.ml_label_dataset import paper_id_to_work_id, sha256_file
from pipeline.ml_offline_baseline_eval import load_label_dataset as load_label_dataset_json
from pipeline.ml_contrastive_review_worksheet import _parse_config_json
from pipeline.recommendation_review_worksheet import (
    _topic_names_from_json,
    cluster_version_from_config,
)
from pipeline.repo_paths import portable_repo_path

WORKSHEET_VERSION = "ml-bridge-negative-mining-v1"
REVIEW_POOL_VARIANT = "ml_bridge_negative_mining_audit"
DEFAULT_RANKING_RUN_ID = "rank-83787b91ef"
DEFAULT_SAMPLE_SEED = 20260531
MIN_ROWS = 1
MAX_ROWS = 200

ALLOWED_SAMPLE_REASONS: tuple[str, ...] = (
    "bridge_deep_cut",
    "bridge_suppressed_final",
    "bridge_score_mismatch",
    "bridge_ineligible_surface",
    "corpus_blind_seeded_fill",
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
)

ABSTRACT_PREVIEW_MAX_CHARS = 360

CAVEATS: tuple[str, ...] = (
    "This worksheet is not validation of bridge ranking quality.",
    "Rows are for offline bridge negative / borderline manual labeling only.",
    "The reviewer CSV intentionally hides ranking scores, ranks, and bridge eligibility flags.",
    "No bridge model is trained, no ranking is run, and no production bridge change is supported.",
)


class MLBridgeNegativeMiningWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class BridgeMiningCandidate:
    family_rank: int
    paper_id: str
    work_token: str
    internal_work_id: int
    title: str
    year: int | None
    citation_count: int
    source_slug: str
    topics_raw: Any
    abstract: str
    final_score: float
    semantic_score: float | None
    citation_velocity_score: float | None
    topic_growth_score: float | None
    bridge_score: float | None
    diversity_penalty: float | None
    bridge_eligible: bool | None
    reason_short: str


@dataclass(frozen=True)
class BridgeMiningSelection:
    candidate: BridgeMiningCandidate
    sample_reason: str


def stable_row_id(*, worksheet_version: str, sample_seed: int, paper_id: str) -> str:
    raw = f"{worksheet_version}|{sample_seed}|{paper_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _norm_ws(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _truncate_abstract(abstract: str, max_chars: int = ABSTRACT_PREVIEW_MAX_CHARS) -> str:
    text = " ".join(abstract.split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _seeded_shuffle(items: Sequence[Any], *, seed: int, salt: str) -> list[Any]:
    out = list(items)
    rng = random.Random(f"{seed}|{salt}")
    rng.shuffle(out)
    return out


def _quantile_sorted(sorted_vals: Sequence[float], q: float) -> float | None:
    if not sorted_vals:
        return None
    n = len(sorted_vals)
    if n == 1:
        return float(sorted_vals[0])
    pos = q * (n - 1)
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return float(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac)


def build_bridge_exclusion_sets(
    payload: dict[str, Any],
    *,
    ranking_run_id: str,
) -> tuple[set[tuple[str, str]], set[tuple[str, str]], set[str]]:
    complete_run, incomplete_run = build_label_key_sets(payload, ranking_run_id=ranking_run_id)
    complete_bridge_any: set[str] = set()
    rows = payload.get("rows")
    if not isinstance(rows, list):
        rows = payload.get("labels")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("family", "")).strip() != "bridge":
                continue
            rel = _norm_ws(row.get("relevance_label"))
            nov = _norm_ws(row.get("novelty_label"))
            br = _norm_ws(row.get("bridge_like_label"))
            if not (rel and nov and br):
                continue
            token = paper_id_to_work_id(_norm_ws(row.get("paper_id")))
            if token:
                complete_bridge_any.add(token.upper())
    return complete_run, incomplete_run, complete_bridge_any


def assert_succeeded_ranking_run(conn: psycopg.Connection, *, ranking_run_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, config_json, status
        FROM ranking_runs
        WHERE ranking_run_id = %s
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        raise MLBridgeNegativeMiningWorksheetError(f"ranking_run_id not found: {ranking_run_id!r}")
    if str(row["status"]) != "succeeded":
        raise MLBridgeNegativeMiningWorksheetError(
            f"ranking run {ranking_run_id!r} is not succeeded (status={row['status']!r})."
        )
    return dict(row)


def fetch_bridge_scored_rows(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
) -> list[dict[str, Any]]:
    query = """
        SELECT
            ROW_NUMBER() OVER (ORDER BY ps.final_score DESC, ps.work_id ASC) AS family_rank,
            w.id AS internal_work_id,
            w.openalex_id AS paper_id,
            w.title,
            w.year,
            w.citation_count,
            w.source_slug,
            COALESCE(w.abstract, '') AS abstract,
            COALESCE(topic_agg.topics, '[]'::json) AS topics,
            ps.final_score,
            ps.reason_short,
            ps.semantic_score,
            ps.citation_velocity_score,
            ps.topic_growth_score,
            ps.bridge_score,
            ps.diversity_penalty,
            ps.bridge_eligible
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        LEFT JOIN LATERAL (
            SELECT json_agg(sub.topic_name ORDER BY sub.score DESC, sub.topic_name ASC) AS topics
            FROM (
                SELECT t.name AS topic_name, wt.score AS score
                FROM work_topics wt
                JOIN topics t ON t.id = wt.topic_id
                WHERE wt.work_id = w.id
                ORDER BY wt.score DESC, t.name ASC
                LIMIT 3
            ) sub
        ) topic_agg ON TRUE
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = 'bridge'
        ORDER BY ps.final_score DESC, ps.work_id ASC
    """
    return list(conn.execute(query, (ranking_run_id,)).fetchall())


def raw_row_to_candidate(row: dict[str, Any]) -> BridgeMiningCandidate | None:
    pid = str(row.get("paper_id") or "")
    wt = paper_id_to_work_id(pid)
    if not wt:
        return None
    fs = row.get("final_score")
    if fs is None:
        return None
    return BridgeMiningCandidate(
        family_rank=int(row["family_rank"]),
        paper_id=pid,
        work_token=wt.upper(),
        internal_work_id=int(row["internal_work_id"]),
        title=str(row.get("title") or ""),
        year=int(row["year"]) if row.get("year") is not None else None,
        citation_count=int(row["citation_count"] or 0),
        source_slug=str(row["source_slug"] or "") if row.get("source_slug") is not None else "",
        topics_raw=row.get("topics"),
        abstract=str(row.get("abstract") or ""),
        final_score=float(fs),
        semantic_score=float(row["semantic_score"]) if row.get("semantic_score") is not None else None,
        citation_velocity_score=float(row["citation_velocity_score"])
        if row.get("citation_velocity_score") is not None
        else None,
        topic_growth_score=float(row["topic_growth_score"]) if row.get("topic_growth_score") is not None else None,
        bridge_score=float(row["bridge_score"]) if row.get("bridge_score") is not None else None,
        diversity_penalty=float(row["diversity_penalty"]) if row.get("diversity_penalty") is not None else None,
        bridge_eligible=None if row.get("bridge_eligible") is None else bool(row["bridge_eligible"]),
        reason_short=str(row.get("reason_short") or ""),
    )


def _eligible_pool(
    candidates: Sequence[BridgeMiningCandidate],
    *,
    complete_run: set[tuple[str, str]],
    complete_bridge_any: set[str],
) -> list[BridgeMiningCandidate]:
    out: list[BridgeMiningCandidate] = []
    for cand in candidates:
        key = paper_key_family(cand.paper_id, "bridge")
        if key in complete_run:
            continue
        if cand.work_token in complete_bridge_any:
            continue
        out.append(cand)
    return out


def select_bridge_negative_sample(
    candidates: Sequence[BridgeMiningCandidate],
    *,
    total_rows: int,
    seed: int,
    deep_cut_min_rank: int = 21,
    deep_cut_max_rank: int = 80,
) -> tuple[list[BridgeMiningSelection], dict[str, Any]]:
    if total_rows < MIN_ROWS:
        raise MLBridgeNegativeMiningWorksheetError(f"requested rows must be at least {MIN_ROWS}")

    pool = list(candidates)
    top20 = [c for c in pool if c.family_rank <= 20]
    top20_scores = [c.final_score for c in top20]
    top20_min = min(top20_scores) if top20_scores else None
    top20_median = statistics.median(top20_scores) if top20_scores else None

    bridge_scores = sorted(c.bridge_score for c in pool if c.bridge_score is not None)
    bridge_p75 = _quantile_sorted(bridge_scores, 0.75) if bridge_scores else None

    deep_cut = [
        c
        for c in pool
        if deep_cut_min_rank <= c.family_rank <= deep_cut_max_rank
    ]
    deep_cut_sorted = sorted(deep_cut, key=lambda c: (c.final_score, c.family_rank, c.work_token))

    suppressed: list[BridgeMiningCandidate] = []
    for c in pool:
        if c.family_rank <= deep_cut_max_rank:
            continue
        if top20_min is not None and c.final_score < top20_min:
            suppressed.append(c)
            continue
        if c.final_score < 0.42:
            suppressed.append(c)
            continue
        if c.family_rank >= 120:
            suppressed.append(c)
    suppressed_sorted = sorted(suppressed, key=lambda c: (c.final_score, c.family_rank, c.work_token))

    mismatch: list[BridgeMiningCandidate] = []
    if bridge_p75 is not None and top20_median is not None:
        for c in pool:
            if c.family_rank <= 20:
                continue
            if c.bridge_score is None:
                continue
            if c.bridge_score >= bridge_p75 and c.final_score < top20_median:
                mismatch.append(c)
    mismatch_sorted = sorted(mismatch, key=lambda c: (c.final_score, c.family_rank, c.work_token))

    ineligible = sorted(
        [c for c in pool if c.bridge_eligible is False],
        key=lambda c: (c.final_score, c.family_rank, c.work_token),
    )

    blind_fill = sorted(
        [c for c in pool if 81 <= c.family_rank <= 300],
        key=lambda c: (c.family_rank, c.work_token),
    )

    bucket_specs: list[tuple[str, list[BridgeMiningCandidate], int]] = [
        ("bridge_deep_cut", deep_cut_sorted, max(24, total_rows // 3)),
        ("bridge_suppressed_final", suppressed_sorted, max(20, total_rows // 3)),
        ("bridge_score_mismatch", mismatch_sorted, 10),
        ("bridge_ineligible_surface", ineligible, 10),
        ("corpus_blind_seeded_fill", blind_fill, max(12, total_rows // 5)),
    ]

    used: set[str] = set()
    selected: list[BridgeMiningSelection] = []

    def take_from(reason: str, seq: Iterable[BridgeMiningCandidate], max_take: int) -> None:
        nonlocal selected
        ordered = _seeded_shuffle(list(seq), seed=seed, salt=f"bucket:{reason}")
        taken = 0
        for cand in ordered:
            if len(selected) >= total_rows or taken >= max_take:
                return
            if cand.work_token in used:
                continue
            selected.append(BridgeMiningSelection(candidate=cand, sample_reason=reason))
            used.add(cand.work_token)
            taken += 1

    for reason, seq, cap in bucket_specs:
        if reason not in ALLOWED_SAMPLE_REASONS:
            raise MLBridgeNegativeMiningWorksheetError(f"internal error: bad sample_reason {reason!r}")
        take_from(reason, seq, cap)

    if len(selected) < total_rows:
        remaining = [c for c in pool if c.work_token not in used]
        remaining_sorted = sorted(remaining, key=lambda c: (-c.family_rank, c.work_token))
        take_from("corpus_blind_seeded_fill", remaining_sorted, total_rows - len(selected))

    selected = [
        BridgeMiningSelection(item.candidate, item.sample_reason)
        for item in _seeded_shuffle(selected, seed=seed, salt="csv_order")
    ][:total_rows]

    reason_counts = Counter(item.sample_reason for item in selected)
    debug = {
        "requested_rows": total_rows,
        "achieved_rows": len(selected),
        "shortfall_count": max(0, total_rows - len(selected)),
        "bridge_pool_size": len(pool),
        "deep_cut_pool_size": len(deep_cut),
        "suppressed_pool_size": len(suppressed),
        "mismatch_pool_size": len(mismatch),
        "ineligible_pool_size": len(ineligible),
        "blind_fill_pool_size": len(blind_fill),
        "top20_min_final_score": top20_min,
        "top20_median_final_score": top20_median,
        "bridge_score_p75": bridge_p75,
        "sample_reason_counts": dict(sorted(reason_counts.items())),
        "family_rank_range": (
            [min(item.candidate.family_rank for item in selected), max(item.candidate.family_rank for item in selected)]
            if selected
            else []
        ),
        "final_score_range": (
            [min(item.candidate.final_score for item in selected), max(item.candidate.final_score for item in selected)]
            if selected
            else []
        ),
    }
    return selected, debug


def _candidate_csv_row(*, selection: BridgeMiningSelection, seed: int) -> dict[str, str]:
    cand = selection.candidate
    topics_list = _topic_names_from_json(cand.topics_raw)
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
    selection: BridgeMiningSelection,
    seed: int,
    run: dict[str, Any],
    cluster_version: str,
) -> dict[str, Any]:
    cand = selection.candidate
    return {
        "row_id": stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id),
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "internal_work_id": cand.internal_work_id,
        "sample_seed": seed,
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
    label_dataset_path: Path,
    label_dataset_sha256: str,
    conflict_policy_path: Path,
    conflict_policy_sha256: str,
    requested_rows: int,
    debug: dict[str, Any],
    seed: int,
    ranking_run_id: str,
) -> dict[str, Any]:
    return {
        "artifact_type": "ml_bridge_negative_mining_v1_context",
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
            "allowed_sample_reasons": list(ALLOWED_SAMPLE_REASONS),
            "deep_cut_rank_window": "21-80",
            "suppressed_final_rule": (
                "rank > 20 and (final_score below top-20 minimum, final_score < 0.42, or rank >= 120)"
            ),
            "bridge_score_mismatch_rule": (
                "rank > 20 and bridge_score >= p75 and final_score below top-20 median when bridge_score exists"
            ),
            "csv_ordering": "seeded hash shuffle over selected rows; not final_score order",
        },
        "sampling_debug": dict(debug),
        "rows": list(sidecar_rows),
    }


def render_markdown(
    *,
    selected: Sequence[BridgeMiningSelection],
    debug: dict[str, Any],
    seed: int,
    run: dict[str, Any],
    cluster_version: str,
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
    achieved = int(debug.get("achieved_rows", len(selected)))
    shortfall = int(debug.get("shortfall_count", max(0, requested_rows - achieved)))
    lines = [
        f"# Bridge negative-mining worksheet (`{WORKSHEET_VERSION}`)",
        "",
        "## Purpose",
        "",
        "Reviewer-blind worksheet for bridge-surface negatives and borderline cases drawn below the heuristic "
        "top band of a persisted bridge ranking run. This is label-collection only, not model training or validation.",
        "",
        "## Provenance",
        "",
        f"- **worksheet_version:** `{WORKSHEET_VERSION}`",
        f"- **review_pool_variant:** `{REVIEW_POOL_VARIANT}`",
        f"- **sample_seed:** `{seed}`",
        f"- **ranking_run_id:** `{run['ranking_run_id']}`",
        f"- **ranking_version:** `{run['ranking_version']}`",
        f"- **corpus_snapshot_version:** `{run['corpus_snapshot_version']}`",
        f"- **embedding_version:** `{run['embedding_version']}`",
        f"- **cluster_version:** `{cluster_version}`",
        f"- **label_dataset:** `{portable_repo_path(label_dataset_path)}`",
        f"- **label_dataset_sha256:** `{label_dataset_sha256}`",
        f"- **conflict_policy:** `{portable_repo_path(conflict_policy_path)}`",
        f"- **csv_output:** `{portable_repo_path(csv_output_path)}`",
        f"- **context_sidecar_output:** `{portable_repo_path(context_output_path)}`",
        "",
        "## Sample Summary",
        "",
        f"- **requested rows:** `{requested_rows}`",
        f"- **achieved rows:** `{achieved}`",
        f"- **shortfall:** `{shortfall}`",
        f"- **bridge pool size:** `{debug.get('bridge_pool_size')}`",
        f"- **top-20 min final_score:** `{debug.get('top20_min_final_score')}`",
        f"- **family_rank range:** `{debug.get('family_rank_range')}`",
        f"- **final_score range:** `{debug.get('final_score_range')}`",
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


def build_bridge_negative_mining_worksheet(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    ranking_run_id: str,
    rows: int,
    seed: int,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
) -> tuple[str, dict[str, Any], str, dict[str, Any]]:
    if rows < MIN_ROWS or rows > MAX_ROWS:
        raise MLBridgeNegativeMiningWorksheetError(f"--rows must be between {MIN_ROWS} and {MAX_ROWS}")
    if not label_dataset_path.is_file():
        raise MLBridgeNegativeMiningWorksheetError(f"label dataset not found: {label_dataset_path}")
    if not conflict_policy_path.is_file():
        raise MLBridgeNegativeMiningWorksheetError(f"conflict policy not found: {conflict_policy_path}")

    rid = str(ranking_run_id).strip()
    if not rid:
        raise MLBridgeNegativeMiningWorksheetError("--ranking-run-id is required and must not be blank")

    payload = load_label_dataset_json(label_dataset_path)
    complete_run, _incomplete_run, complete_bridge_any = build_bridge_exclusion_sets(payload, ranking_run_id=rid)
    label_sha = sha256_file(label_dataset_path)
    conflict_sha = sha256_file(conflict_policy_path)

    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        run = assert_succeeded_ranking_run(conn, ranking_run_id=rid)
        cfg = _parse_config_json(run.get("config_json"))
        cluster_ver = cluster_version_from_config(cfg) or ""
        raw_rows = fetch_bridge_scored_rows(conn, ranking_run_id=rid)

    candidates: list[BridgeMiningCandidate] = []
    for raw in raw_rows:
        cand = raw_row_to_candidate(dict(raw))
        if cand is not None:
            candidates.append(cand)

    pool = _eligible_pool(candidates, complete_run=complete_run, complete_bridge_any=complete_bridge_any)
    if not pool:
        raise MLBridgeNegativeMiningWorksheetError(
            f"no eligible bridge candidates remain after label exclusions for {rid!r}"
        )

    selected, debug = select_bridge_negative_sample(pool, total_rows=rows, seed=seed)
    debug["excluded_complete_on_run"] = len(complete_run)
    debug["excluded_complete_bridge_any"] = len(complete_bridge_any)
    debug["eligible_pool_size"] = len(pool)

    csv_rows = [_candidate_csv_row(selection=item, seed=seed) for item in selected]
    sidecar_rows = [
        _candidate_sidecar_row(selection=item, seed=seed, run=run, cluster_version=cluster_ver)
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
        ranking_run_id=rid,
    )
    md_text = render_markdown(
        selected=selected,
        debug=debug,
        seed=seed,
        run=run,
        cluster_version=cluster_ver,
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


def run_ml_bridge_negative_mining_worksheet_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    ranking_run_id: str,
    rows: int,
    seed: int,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
) -> dict[str, Any]:
    csv_text, context_payload, md_text, debug = build_bridge_negative_mining_worksheet(
        database_url=database_url,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
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
    "DEFAULT_RANKING_RUN_ID",
    "DEFAULT_SAMPLE_SEED",
    "MLBridgeNegativeMiningWorksheetError",
    "REVIEW_POOL_VARIANT",
    "WORKSHEET_VERSION",
    "build_bridge_exclusion_sets",
    "build_bridge_negative_mining_worksheet",
    "render_csv",
    "render_markdown",
    "run_ml_bridge_negative_mining_worksheet_cli",
    "select_bridge_negative_sample",
    "stable_row_id",
]
