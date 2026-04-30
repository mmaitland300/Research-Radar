"""Deterministic, non-rank-driven blind snapshot review worksheet (read-only DB; no training, no ranking, no writes)."""

from __future__ import annotations

import csv
import io
import json
import random
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.repo_paths import portable_repo_path
from pipeline.ml_label_dataset import LABEL_FIELDS, paper_id_to_work_id
from pipeline.recommendation_review_worksheet import _topic_names_from_json

WORKSHEET_VERSION = "ml-blind-snapshot-review-v1"
REVIEW_POOL_VARIANT = "ml_blind_snapshot_audit"

ALLOWED_SAMPLE_REASONS: tuple[str, ...] = (
    "cluster_stratified_seeded",
    "year_band_seeded",
    "citation_band_seeded",
    "weak_family_context_seeded",
    "cluster_undercovered_seeded",
    "fallback_seeded_fill",
)

CSV_COLUMNS: tuple[str, ...] = (
    "worksheet_version",
    "sample_seed",
    "sample_reason",
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
    "ranking_run_id_context",
    "review_pool_variant",
    "paper_id",
    "openalex_work_id",
    "internal_work_id",
    "title",
    "year",
    "citation_count",
    "source_slug",
    "type",
    "cluster_id",
    "topics",
    "abstract_preview",
    "ranking_context_family_scores_json",
    "ranking_context_family_ranks_json",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)

VERBATIM_CAVEATS: tuple[str, ...] = (
    "This worksheet is not validation of ranking quality.",
    "Rows are sampled for offline audit labeling, not training or production use.",
    "The split remains audit_only until a deliberate train/dev/test policy exists.",
    "Ranking context fields are provided for provenance and reviewer context; they must not be treated as labels.",
)

ABSTRACT_PREVIEW_MAX_CHARS = 360
MIN_ROWS = 1
MAX_ROWS = 500


class MLBlindSnapshotReviewWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class BlindCandidate:
    internal_work_id: int
    paper_id: str
    work_token: str
    title: str
    year: int | None
    citation_count: int
    source_slug: str
    work_type: str
    cluster_id: str
    topics: tuple[str, ...]
    abstract: str
    family_scores: dict[str, float]
    family_ranks: dict[str, int]


def _norm_ws(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _truncate_abstract(abstract: str, max_chars: int = ABSTRACT_PREVIEW_MAX_CHARS) -> str:
    text = " ".join(abstract.split())
    if len(text) <= max_chars:
        return text
    cut = text[: max_chars - 3].rstrip()
    return cut + "..."


def _portable_path_display(path: Path) -> str:
    """Render path for committed artifacts without machine-specific absolutes when possible."""
    return portable_repo_path(path)


def _year_band(year: int | None) -> str:
    if year is None:
        return "year_missing"
    if year <= 2018:
        return "year_le_2018"
    if year <= 2020:
        return "year_2019_2020"
    if year <= 2022:
        return "year_2021_2022"
    if year <= 2024:
        return "year_2023_2024"
    return "year_ge_2025"


def _citation_band(count: int) -> str:
    if count <= 0:
        return "cite_0"
    if count <= 9:
        return "cite_1_9"
    if count <= 49:
        return "cite_10_49"
    if count <= 199:
        return "cite_50_199"
    return "cite_ge_200"


def load_label_dataset_payload(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLBlindSnapshotReviewWorksheetError(f"Failed to load label dataset {path}: {exc}") from exc


def fully_labeled_work_tokens(payload: dict[str, Any]) -> set[str]:
    """Set of uppercase W tokens that have any row with all three manual labels filled (any family)."""
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return set()
    out: set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        rel = _norm_ws(r.get("relevance_label"))
        nov = _norm_ws(r.get("novelty_label"))
        br = _norm_ws(r.get("bridge_like_label"))
        if not (rel and nov and br):
            continue
        wt = _norm_ws(r.get("work_id"))
        if wt and wt.upper().startswith("W"):
            out.add(wt.upper())
            continue
        derived = paper_id_to_work_id(_norm_ws(r.get("paper_id")))
        if derived:
            out.add(derived.upper())
    return out


def fetch_candidate_pool(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    cluster_version: str,
) -> list[dict[str, Any]]:
    q = """
        SELECT
            w.id AS internal_work_id,
            w.openalex_id AS paper_id,
            w.title,
            w.year,
            w.citation_count,
            w.source_slug,
            w.type AS work_type,
            COALESCE(w.abstract, '') AS abstract,
            c.cluster_id,
            COALESCE(topic_agg.topics, '[]'::json) AS topics
        FROM works w
        JOIN clusters c ON c.work_id = w.id AND c.cluster_version = %s
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
        WHERE w.corpus_snapshot_version = %s
          AND w.inclusion_status = 'included'
        ORDER BY w.id ASC
    """
    return list(conn.execute(q, (cluster_version, corpus_snapshot_version)).fetchall())


def fetch_ranking_context(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
) -> dict[int, tuple[dict[str, float], dict[str, int]]]:
    """Map internal work_id -> ({family: final_score}, {family: family_rank})."""
    q = """
        SELECT
            ps.work_id,
            ps.recommendation_family,
            ps.final_score,
            RANK() OVER (
                PARTITION BY ps.recommendation_family
                ORDER BY ps.final_score DESC, ps.work_id ASC
            ) AS family_rank
        FROM paper_scores ps
        WHERE ps.ranking_run_id = %s
        ORDER BY ps.recommendation_family ASC, family_rank ASC
    """
    rows = list(conn.execute(q, (ranking_run_id,)).fetchall())
    out: dict[int, tuple[dict[str, float], dict[str, int]]] = {}
    for r in rows:
        wid = int(r["work_id"])
        fam = str(r["recommendation_family"])
        score = float(r["final_score"]) if r["final_score"] is not None else 0.0
        rank = int(r["family_rank"])
        if wid not in out:
            out[wid] = ({}, {})
        scores, ranks = out[wid]
        scores[fam] = score
        ranks[fam] = rank
    return out


def assert_succeeded_ranking_run(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    expected_corpus_snapshot_version: str,
    expected_embedding_version: str,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, status
        FROM ranking_runs
        WHERE ranking_run_id = %s
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        raise MLBlindSnapshotReviewWorksheetError(f"ranking_run_id not found: {ranking_run_id!r}")
    d = dict(row)
    if str(d["status"]) != "succeeded":
        raise MLBlindSnapshotReviewWorksheetError(
            f"ranking run {ranking_run_id!r} is not succeeded (status={d['status']!r})."
        )
    if str(d["corpus_snapshot_version"]) != expected_corpus_snapshot_version:
        raise MLBlindSnapshotReviewWorksheetError(
            "ranking run corpus_snapshot_version mismatch: "
            f"run={d['corpus_snapshot_version']!r} expected={expected_corpus_snapshot_version!r}"
        )
    if str(d["embedding_version"]) != expected_embedding_version:
        raise MLBlindSnapshotReviewWorksheetError(
            "ranking run embedding_version mismatch: "
            f"run={d['embedding_version']!r} expected={expected_embedding_version!r}"
        )
    return d


def assert_succeeded_clustering_run(
    conn: psycopg.Connection,
    *,
    cluster_version: str,
    expected_corpus_snapshot_version: str,
    expected_embedding_version: str,
) -> None:
    row = conn.execute(
        """
        SELECT corpus_snapshot_version, embedding_version, status
        FROM clustering_runs
        WHERE cluster_version = %s
        """,
        (cluster_version,),
    ).fetchone()
    if row is None:
        raise MLBlindSnapshotReviewWorksheetError(f"cluster_version not found: {cluster_version!r}")
    d = dict(row)
    if str(d["status"]) != "succeeded":
        raise MLBlindSnapshotReviewWorksheetError(
            f"clustering run {cluster_version!r} is not succeeded (status={d['status']!r})."
        )
    if str(d["corpus_snapshot_version"]) != expected_corpus_snapshot_version:
        raise MLBlindSnapshotReviewWorksheetError(
            "cluster corpus_snapshot_version mismatch: "
            f"run={d['corpus_snapshot_version']!r} expected={expected_corpus_snapshot_version!r}"
        )
    if str(d["embedding_version"]) != expected_embedding_version:
        raise MLBlindSnapshotReviewWorksheetError(
            "cluster embedding_version mismatch: "
            f"run={d['embedding_version']!r} expected={expected_embedding_version!r}"
        )


def raw_pool_to_candidates(
    raw_rows: Sequence[dict[str, Any]],
    *,
    ranking_context: dict[int, tuple[dict[str, float], dict[str, int]]],
) -> list[BlindCandidate]:
    out: list[BlindCandidate] = []
    for r in raw_rows:
        pid = str(r.get("paper_id") or "")
        wt = paper_id_to_work_id(pid)
        if not wt:
            continue
        wid = int(r["internal_work_id"])
        topics = tuple(_topic_names_from_json(r.get("topics")))
        scores, ranks = ranking_context.get(wid, ({}, {}))
        out.append(
            BlindCandidate(
                internal_work_id=wid,
                paper_id=pid,
                work_token=wt.upper(),
                title=str(r.get("title") or ""),
                year=int(r["year"]) if r.get("year") is not None else None,
                citation_count=int(r.get("citation_count") or 0),
                source_slug=str(r.get("source_slug") or ""),
                work_type=str(r.get("work_type") or ""),
                cluster_id=str(r.get("cluster_id") or ""),
                topics=topics,
                abstract=str(r.get("abstract") or ""),
                family_scores=dict(scores),
                family_ranks=dict(ranks),
            )
        )
    return out


def _budgets(total: int, num_clusters: int) -> dict[str, int]:
    if num_clusters < 1:
        raise MLBlindSnapshotReviewWorksheetError("num_clusters must be >= 1")
    p1 = max(num_clusters, (total * 4) // 5)
    if p1 > total:
        p1 = total
    rem = total - p1
    per_extra = rem // 3
    p2 = per_extra
    p3 = per_extra
    p4 = rem - p2 - p3
    return {
        "cluster_stratified_seeded": p1,
        "year_band_seeded": p2,
        "citation_band_seeded": p3,
        "weak_family_context_seeded": p4,
    }


def _seeded_shuffle(seq: Sequence[Any], *, seed: int, salt: str) -> list[Any]:
    rng = random.Random(f"{seed}|{salt}")
    out = list(seq)
    rng.shuffle(out)
    return out


def _mean_family_score(c: BlindCandidate) -> float | None:
    if not c.family_scores:
        return None
    vals = [float(v) for v in c.family_scores.values() if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


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


def _build_weak_predicate(eligible: Sequence[BlindCandidate]) -> Callable[[BlindCandidate], bool]:
    """Threshold-based weak-context predicate using the eligible pool's bottom-quartile mean score.

    A candidate qualifies if it has no family scores at all (missing) OR if every family
    score is None/0 OR if its mean family score sits at-or-below the bottom-quartile threshold.
    Threshold is computed once over the eligible pool; selection within the stratum is seeded.
    """
    means = [m for m in (_mean_family_score(c) for c in eligible) if m is not None]
    threshold: float | None = None
    if means:
        threshold = _quantile_sorted(sorted(means), 0.25)

    def predicate(c: BlindCandidate) -> bool:
        if not c.family_scores:
            return True
        if all((v is None or v == 0.0) for v in c.family_scores.values()):
            return True
        if threshold is None:
            return False
        m = _mean_family_score(c)
        return m is not None and m <= threshold

    return predicate


def select_blind_sample(
    pool: Sequence[BlindCandidate],
    *,
    fully_labeled_tokens: set[str],
    total_rows: int,
    seed: int,
) -> tuple[list[tuple[BlindCandidate, str]], dict[str, Any]]:
    """Pure deterministic seeded sampling. Never sorts by final_score; uses cluster + band + weak strata."""
    if total_rows < MIN_ROWS or total_rows > MAX_ROWS:
        raise MLBlindSnapshotReviewWorksheetError(
            f"--rows must be between {MIN_ROWS} and {MAX_ROWS}"
        )
    eligible = [c for c in pool if c.work_token not in fully_labeled_tokens]
    eligible_sorted = sorted(eligible, key=lambda c: c.work_token)

    by_cluster: dict[str, list[BlindCandidate]] = defaultdict(list)
    for c in eligible_sorted:
        by_cluster[c.cluster_id].append(c)
    cluster_ids = sorted(by_cluster.keys())
    num_clusters = max(1, len(cluster_ids))

    pool_pool_size = len(eligible_sorted)
    target = min(total_rows, pool_pool_size)

    budgets = _budgets(target, num_clusters)
    selected: list[tuple[BlindCandidate, str]] = []
    used: set[str] = set()

    def _take(cands: Sequence[BlindCandidate], reason: str, *, max_take: int) -> int:
        taken = 0
        for c in cands:
            if taken >= max_take or len(selected) >= target:
                break
            if c.work_token in used:
                continue
            selected.append((c, reason))
            used.add(c.work_token)
            taken += 1
        return taken

    per_cluster = budgets["cluster_stratified_seeded"] // num_clusters
    leftover_p1 = budgets["cluster_stratified_seeded"] - per_cluster * num_clusters
    p1_taken = 0
    for cid in cluster_ids:
        if len(selected) >= target:
            break
        cluster_pool = _seeded_shuffle(by_cluster[cid], seed=seed, salt=f"cluster:{cid}")
        p1_taken += _take(cluster_pool, "cluster_stratified_seeded", max_take=per_cluster)
    if leftover_p1 > 0 and len(selected) < target:
        rotation = _seeded_shuffle(cluster_ids, seed=seed, salt="leftover_clusters")
        for cid in rotation:
            if leftover_p1 <= 0 or len(selected) >= target:
                break
            cluster_pool = _seeded_shuffle(by_cluster[cid], seed=seed, salt=f"cluster_extra:{cid}")
            t = _take(cluster_pool, "cluster_stratified_seeded", max_take=1)
            leftover_p1 -= t
            p1_taken += t

    by_year_band: dict[str, list[BlindCandidate]] = defaultdict(list)
    for c in eligible_sorted:
        if c.work_token in used:
            continue
        by_year_band[_year_band(c.year)].append(c)
    year_bands_sorted = sorted(by_year_band.keys())
    p2_budget = budgets["year_band_seeded"]
    for band in _seeded_shuffle(year_bands_sorted, seed=seed, salt="year_bands_order"):
        if p2_budget <= 0 or len(selected) >= target:
            break
        band_pool = _seeded_shuffle(by_year_band[band], seed=seed, salt=f"year_band:{band}")
        t = _take(band_pool, "year_band_seeded", max_take=1)
        p2_budget -= t

    by_cite_band: dict[str, list[BlindCandidate]] = defaultdict(list)
    for c in eligible_sorted:
        if c.work_token in used:
            continue
        by_cite_band[_citation_band(c.citation_count)].append(c)
    cite_bands_sorted = sorted(by_cite_band.keys())
    p3_budget = budgets["citation_band_seeded"]
    for band in _seeded_shuffle(cite_bands_sorted, seed=seed, salt="cite_bands_order"):
        if p3_budget <= 0 or len(selected) >= target:
            break
        band_pool = _seeded_shuffle(by_cite_band[band], seed=seed, salt=f"cite_band:{band}")
        t = _take(band_pool, "citation_band_seeded", max_take=1)
        p3_budget -= t

    weak_predicate = _build_weak_predicate(eligible_sorted)
    weak_pool = [c for c in eligible_sorted if c.work_token not in used and weak_predicate(c)]
    weak_shuffled = _seeded_shuffle(weak_pool, seed=seed, salt="weak_family_context")
    _take(weak_shuffled, "weak_family_context_seeded", max_take=budgets["weak_family_context_seeded"])

    while len(selected) < target:
        cluster_taken: Counter[str] = Counter()
        for c, _r in selected:
            cluster_taken[c.cluster_id] += 1
        under_clusters = [cid for cid in cluster_ids if cluster_taken[cid] < per_cluster]
        if not under_clusters:
            break
        progress = 0
        for cid in _seeded_shuffle(under_clusters, seed=seed, salt="undercovered_round"):
            if len(selected) >= target:
                break
            cluster_pool = _seeded_shuffle(
                [c for c in by_cluster[cid] if c.work_token not in used],
                seed=seed,
                salt=f"undercovered:{cid}",
            )
            progress += _take(cluster_pool, "cluster_undercovered_seeded", max_take=1)
        if progress == 0:
            break

    if len(selected) < target:
        remaining = [c for c in eligible_sorted if c.work_token not in used]
        remaining = _seeded_shuffle(remaining, seed=seed, salt="fallback_fill")
        _take(remaining, "fallback_seeded_fill", max_take=target - len(selected))

    debug = {
        "eligible_pool_size": pool_pool_size,
        "fully_labeled_excluded_count": sum(1 for c in pool if c.work_token in fully_labeled_tokens),
        "cluster_count": num_clusters,
        "cluster_ids": cluster_ids,
        "budgets": budgets,
        "target_rows": target,
        "achieved_rows": len(selected),
    }
    return selected, debug


def _row_to_csv_dict(
    *,
    cand: BlindCandidate,
    sample_reason: str,
    seed: int,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id_context: str,
) -> dict[str, str]:
    fam_scores_json = json.dumps(
        {k: cand.family_scores[k] for k in sorted(cand.family_scores)},
        sort_keys=True,
    ) if cand.family_scores else ""
    fam_ranks_json = json.dumps(
        {k: cand.family_ranks[k] for k in sorted(cand.family_ranks)},
        sort_keys=True,
    ) if cand.family_ranks else ""
    return {
        "worksheet_version": WORKSHEET_VERSION,
        "sample_seed": str(seed),
        "sample_reason": sample_reason,
        "corpus_snapshot_version": corpus_snapshot_version,
        "embedding_version": embedding_version,
        "cluster_version": cluster_version,
        "ranking_run_id_context": ranking_run_id_context,
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "internal_work_id": str(cand.internal_work_id),
        "title": cand.title,
        "year": str(int(cand.year)) if cand.year is not None else "",
        "citation_count": str(int(cand.citation_count)),
        "source_slug": cand.source_slug,
        "type": cand.work_type,
        "cluster_id": cand.cluster_id,
        "topics": ";".join(cand.topics) if cand.topics else "",
        "abstract_preview": _truncate_abstract(cand.abstract) if cand.abstract else "",
        "ranking_context_family_scores_json": fam_scores_json,
        "ranking_context_family_ranks_json": fam_ranks_json,
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def render_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    for r in rows:
        w.writerow({c: r.get(c, "") for c in CSV_COLUMNS})
    return buf.getvalue()


def render_markdown(
    *,
    csv_rows: Sequence[dict[str, str]],
    selected: Sequence[tuple[BlindCandidate, str]],
    debug: dict[str, Any],
    seed: int,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id_context: str,
    label_dataset_path: Path,
    csv_output_path: Path,
    markdown_output_path: Path,
    requested_rows: int,
) -> str:
    by_reason: Counter[str] = Counter(sr for _c, sr in selected)
    by_cluster: Counter[str] = Counter(c.cluster_id for c, _sr in selected)
    by_year_band: Counter[str] = Counter(_year_band(c.year) for c, _sr in selected)
    by_cite_band: Counter[str] = Counter(_citation_band(c.citation_count) for c, _sr in selected)

    ds_disp = _portable_path_display(label_dataset_path)
    csv_disp = _portable_path_display(csv_output_path)
    md_disp = _portable_path_display(markdown_output_path)

    pool_size = int(debug["eligible_pool_size"])
    excluded_labeled = int(debug["fully_labeled_excluded_count"])
    target = int(debug["target_rows"])
    achieved = int(debug["achieved_rows"])

    short_state_lines: list[str] = []
    if achieved < requested_rows:
        short_state_lines.append(
            f"- **Reduced row count**: requested {requested_rows} but only {achieved} eligible unlabeled rows were available "
            f"after exclusions (eligible pool = {pool_size})."
        )

    lines = [
        f"# Blind snapshot review worksheet (`{WORKSHEET_VERSION}`)",
        "",
        "## Purpose",
        "",
        "Deterministic, **non-rank-driven** sample of candidate works from the corpus snapshot for offline manual labeling. ",
        "Rows are selected by cluster, year, citation, and weak-context strata using a seeded RNG; **not** by `final_score` "
        "ordering or top-k ranking heads.",
        "",
        "## Provenance",
        "",
        f"- **worksheet_version:** `{WORKSHEET_VERSION}`",
        f"- **sample_seed:** `{seed}`",
        f"- **corpus_snapshot_version:** `{corpus_snapshot_version}`",
        f"- **embedding_version:** `{embedding_version}`",
        f"- **cluster_version:** `{cluster_version}`",
        f"- **ranking_run_id_context:** `{ranking_run_id_context}`",
        f"- **label_dataset:** `{ds_disp}`",
        f"- **csv_output:** `{csv_disp}`",
        f"- **markdown_output:** `{md_disp}`",
        f"- **generated_at:** `{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}`",
        "",
        "## Command",
        "",
        "```",
        "python -m pipeline.cli ml-blind-snapshot-review-worksheet \\",
        f"  --label-dataset {ds_disp} \\",
        f"  --corpus-snapshot-version {corpus_snapshot_version} \\",
        f"  --embedding-version {embedding_version} \\",
        f"  --cluster-version {cluster_version} \\",
        f"  --ranking-run-id {ranking_run_id_context} \\",
        f"  --rows {requested_rows} \\",
        f"  --seed {seed} \\",
        f"  --output {csv_disp} \\",
        f"  --markdown-output {md_disp}",
        "```",
        "",
        "## Sample summary",
        "",
        f"- **Requested rows:** {requested_rows}",
        f"- **Achieved rows:** {achieved} (target after exclusions: {target})",
        f"- **Eligible unlabeled pool size:** {pool_size}",
        f"- **Excluded as already fully labeled:** {excluded_labeled}",
    ]
    if short_state_lines:
        lines.append("")
        lines.extend(short_state_lines)
    lines.extend(
        [
            "",
            "## Row counts by sample_reason",
            "",
            "| sample_reason | rows |",
            "| --- | ---: |",
            *[f"| `{r}` | {by_reason[r]} |" for r in ALLOWED_SAMPLE_REASONS if by_reason[r] > 0],
            "",
            "## Row counts by cluster_id",
            "",
            "| cluster_id | rows |",
            "| --- | ---: |",
            *[f"| `{cid}` | {by_cluster[cid]} |" for cid in sorted(by_cluster.keys())],
            "",
            "## Row counts by year band",
            "",
            "| year_band | rows |",
            "| --- | ---: |",
            *[f"| `{band}` | {by_year_band[band]} |" for band in sorted(by_year_band.keys())],
            "",
            "## Row counts by citation band",
            "",
            "| citation_band | rows |",
            "| --- | ---: |",
            *[f"| `{band}` | {by_cite_band[band]} |" for band in sorted(by_cite_band.keys())],
            "",
            "## Caveats",
            "",
            *[f"- {c}" for c in VERBATIM_CAVEATS],
            "",
        ]
    )
    return "\n".join(lines)


def build_blind_snapshot_review_worksheet(
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
    markdown_output_path: Path,
) -> tuple[str, str, dict[str, Any]]:
    if not corpus_snapshot_version.strip():
        raise MLBlindSnapshotReviewWorksheetError("--corpus-snapshot-version is required and must not be blank")
    if not embedding_version.strip():
        raise MLBlindSnapshotReviewWorksheetError("--embedding-version is required and must not be blank")
    if not cluster_version.strip():
        raise MLBlindSnapshotReviewWorksheetError("--cluster-version is required and must not be blank")
    if not ranking_run_id.strip():
        raise MLBlindSnapshotReviewWorksheetError("--ranking-run-id is required and must not be blank")
    if rows < MIN_ROWS or rows > MAX_ROWS:
        raise MLBlindSnapshotReviewWorksheetError(f"--rows must be between {MIN_ROWS} and {MAX_ROWS}")
    if not label_dataset_path.is_file():
        raise MLBlindSnapshotReviewWorksheetError(f"label dataset not found: {label_dataset_path}")

    payload = load_label_dataset_payload(label_dataset_path)
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

    candidates = raw_pool_to_candidates(raw_rows, ranking_context=ranking_context)
    selected, debug = select_blind_sample(
        candidates,
        fully_labeled_tokens=fully_labeled,
        total_rows=rows,
        seed=seed,
    )
    csv_rows = [
        _row_to_csv_dict(
            cand=c,
            sample_reason=sr,
            seed=seed,
            corpus_snapshot_version=corpus_snapshot_version,
            embedding_version=embedding_version,
            cluster_version=cluster_version,
            ranking_run_id_context=ranking_run_id,
        )
        for c, sr in selected
    ]
    csv_text = render_csv(csv_rows)
    md_text = render_markdown(
        csv_rows=csv_rows,
        selected=selected,
        debug=debug,
        seed=seed,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        cluster_version=cluster_version,
        ranking_run_id_context=ranking_run_id,
        label_dataset_path=label_dataset_path,
        csv_output_path=csv_output_path,
        markdown_output_path=markdown_output_path,
        requested_rows=rows,
    )
    return csv_text, md_text, debug


def run_ml_blind_snapshot_review_worksheet_cli(
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
    markdown_output_path: Path,
) -> dict[str, Any]:
    csv_text, md_text, debug = build_blind_snapshot_review_worksheet(
        database_url=database_url,
        label_dataset_path=label_dataset_path,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        cluster_version=cluster_version,
        ranking_run_id=ranking_run_id,
        rows=rows,
        seed=seed,
        csv_output_path=csv_output_path,
        markdown_output_path=markdown_output_path,
    )
    csv_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    csv_output_path.write_text(csv_text, encoding="utf-8", newline="")
    markdown_output_path.write_text(md_text, encoding="utf-8", newline="")
    return debug


__all__ = [
    "ALLOWED_SAMPLE_REASONS",
    "ABSTRACT_PREVIEW_MAX_CHARS",
    "BlindCandidate",
    "CSV_COLUMNS",
    "LABEL_FIELDS",
    "MAX_ROWS",
    "MIN_ROWS",
    "MLBlindSnapshotReviewWorksheetError",
    "REVIEW_POOL_VARIANT",
    "VERBATIM_CAVEATS",
    "WORKSHEET_VERSION",
    "build_blind_snapshot_review_worksheet",
    "fetch_candidate_pool",
    "fetch_ranking_context",
    "fully_labeled_work_tokens",
    "load_label_dataset_payload",
    "raw_pool_to_candidates",
    "render_csv",
    "render_markdown",
    "run_ml_blind_snapshot_review_worksheet_cli",
    "select_blind_sample",
]
