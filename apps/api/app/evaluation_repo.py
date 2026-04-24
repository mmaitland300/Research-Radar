"""Evaluation v0: same candidate pool, three orderings (ranked vs baselines) and proxy stats only."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from math import fsum
from statistics import median
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.papers_repo import _topic_names_from_json
from app.scores_repo import (
    VALID_FAMILIES,
    resolve_ranked_run_context,
    latest_corpus_snapshot_version_with_works,
)

def _low_cite_params_from_run_config(config: dict[str, Any]) -> tuple[int, int]:
    scope = config.get("selection_scope") if isinstance(config.get("selection_scope"), dict) else {}
    uc = scope.get("undercited") if isinstance(scope.get("undercited"), dict) else {}
    min_year = int(uc.get("min_year", 2019))
    max_citations = int(uc.get("max_citations", 30))
    return min_year, max_citations


def _parse_config_row(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _fetch_ranking_run_row(
    conn: psycopg.Connection, *, ranking_run_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, config_json, status
        FROM ranking_runs
        WHERE ranking_run_id = %s AND status = 'succeeded'
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


@dataclass(frozen=True)
class EvalPaperRow:
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    topics: list[str]
    final_score: float | None = None


@dataclass(frozen=True)
class EvalRecencyProxy:
    mean_year: float
    min_year: int
    max_year: int
    share_in_latest_two_years: float


@dataclass(frozen=True)
class EvalCitationProxy:
    mean: float
    median: float
    min_val: int
    max_val: int


@dataclass(frozen=True)
class EvalTopicMixProxy:
    unique_topic_labels: int
    top_topics: list[str]


@dataclass(frozen=True)
class EvalListArm:
    arm_label: str
    arm_description: str
    ordering_description: str
    items: list[EvalPaperRow]
    recency: EvalRecencyProxy
    citations: EvalCitationProxy
    topics: EvalTopicMixProxy


@dataclass(frozen=True)
class EvalTopicOverlap:
    jaccard_ranked_vs_citation_baseline: float
    jaccard_ranked_vs_date_baseline: float
    jaccard_citation_vs_date_baseline: float


@dataclass(frozen=True)
class EvalComparePayload:
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    family: str
    pool_definition: str
    pool_size: int
    low_cite_min_year: int | None
    low_cite_max_citations: int | None
    candidate_pool_doc_revision: str | None
    ranked: EvalListArm
    citation_baseline: EvalListArm
    date_baseline: EvalListArm
    topic_overlap: EvalTopicOverlap


def _topic_label_set(items: list[EvalPaperRow]) -> set[str]:
    out: set[str] = set()
    for it in items:
        for t in it.topics:
            if t:
                out.add(t)
    return out


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    u = a | b
    if not u:
        return 1.0
    return len(a & b) / len(u)


def _row_to_eval_paper(row: dict[str, Any], *, final_score: float | None) -> EvalPaperRow:
    return EvalPaperRow(
        paper_id=str(row["openalex_id"]),
        title=str(row["title"]),
        year=int(row["year"]),
        citation_count=int(row["citation_count"] or 0),
        source_slug=str(row["source_slug"]) if row.get("source_slug") is not None else None,
        topics=_topic_names_from_json(row.get("topics")),
        final_score=final_score,
    )


def _arm_stats(
    items: list[EvalPaperRow], *, arm_label: str, arm_desc: str, ordering_desc: str
) -> EvalListArm:
    years = [it.year for it in items]
    cites = [it.citation_count for it in items]
    if years:
        ymax = max(years)
        share_two = fsum(1.0 for y in years if y >= ymax - 1) / float(len(years))
        recency = EvalRecencyProxy(
            mean_year=fsum(float(y) for y in years) / len(years),
            min_year=min(years),
            max_year=ymax,
            share_in_latest_two_years=round(share_two, 4),
        )
    else:
        recency = EvalRecencyProxy(
            mean_year=0.0, min_year=0, max_year=0, share_in_latest_two_years=0.0
        )

    if cites:
        cit = EvalCitationProxy(
            mean=round(fsum(float(c) for c in cites) / len(cites), 4),
            median=float(median(cites)),
            min_val=min(cites),
            max_val=max(cites),
        )
    else:
        cit = EvalCitationProxy(mean=0.0, median=0.0, min_val=0, max_val=0)

    tc: Counter[str] = Counter()
    for it in items:
        for t in it.topics:
            if t:
                tc[t] += 1
    top_topics = [name for name, _ in tc.most_common(5)]
    topic_mix = EvalTopicMixProxy(unique_topic_labels=len(tc), top_topics=top_topics)

    return EvalListArm(
        arm_label=arm_label,
        arm_description=arm_desc,
        ordering_description=ordering_desc,
        items=items,
        recency=recency,
        citations=cit,
        topics=topic_mix,
    )


def _pool_cte_sql(
    *,
    family: str,
    corpus_snapshot_version: str,
    low_cite_min_year: int,
    low_cite_max_citations: int,
) -> tuple[str, tuple[Any, ...]]:
    if family == "undercited":
        sql = f"""
        WITH pool AS (
            SELECT w.id, w.openalex_id, w.title, w.year, w.citation_count, w.source_slug
            FROM works w
            WHERE w.inclusion_status = 'included'
              AND w.corpus_snapshot_version = %s
              AND w.is_core_corpus = TRUE
              AND w.year >= %s
              AND w.citation_count <= %s
              AND length(trim(COALESCE(w.title, ''))) > 0
              AND length(trim(COALESCE(w.abstract, ''))) > 0
        )
        SELECT COUNT(*)::bigint AS pool_n FROM pool
        """
        params = (corpus_snapshot_version, low_cite_min_year, low_cite_max_citations)
        return sql, params

    sql = """
    WITH pool AS (
        SELECT w.id, w.openalex_id, w.title, w.year, w.citation_count, w.source_slug
        FROM works w
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
    )
    SELECT COUNT(*)::bigint AS pool_n FROM pool
    """
    return sql, (corpus_snapshot_version,)


def _select_from_pool(
    *,
    order_clause: str,
    limit: int,
    corpus_snapshot_version: str,
    family: str,
    low_cite_min_year: int,
    low_cite_max_citations: int,
) -> tuple[str, tuple[Any, ...]]:
    topic_lateral = """
    LEFT JOIN LATERAL (
        SELECT json_agg(sub.topic_name ORDER BY sub.score DESC, sub.topic_name ASC) AS topics
        FROM (
            SELECT t.name AS topic_name, wt.score AS score
            FROM work_topics wt
            JOIN topics t ON t.id = wt.topic_id
            WHERE wt.work_id = pool.id
            ORDER BY wt.score DESC, t.name ASC
            LIMIT 3
        ) sub
    ) topic_agg ON TRUE
    """
    if family == "undercited":
        sql = f"""
        WITH pool AS (
            SELECT w.id, w.openalex_id, w.title, w.year, w.citation_count, w.source_slug
            FROM works w
            WHERE w.inclusion_status = 'included'
              AND w.corpus_snapshot_version = %s
              AND w.is_core_corpus = TRUE
              AND w.year >= %s
              AND w.citation_count <= %s
              AND length(trim(COALESCE(w.title, ''))) > 0
              AND length(trim(COALESCE(w.abstract, ''))) > 0
        )
        SELECT pool.openalex_id, pool.title, pool.year, pool.citation_count, pool.source_slug,
               COALESCE(topic_agg.topics, '[]'::json) AS topics
        FROM pool
        {topic_lateral}
        ORDER BY {order_clause}
        LIMIT %s
        """
        params = (
            corpus_snapshot_version,
            low_cite_min_year,
            low_cite_max_citations,
            limit,
        )
        return sql, params

    sql = f"""
    WITH pool AS (
        SELECT w.id, w.openalex_id, w.title, w.year, w.citation_count, w.source_slug
        FROM works w
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
    )
    SELECT pool.openalex_id, pool.title, pool.year, pool.citation_count, pool.source_slug,
           COALESCE(topic_agg.topics, '[]'::json) AS topics
    FROM pool
    {topic_lateral}
    ORDER BY {order_clause}
    LIMIT %s
    """
    return sql, (corpus_snapshot_version, limit)


def load_evaluation_compare(
    *,
    database_url: str,
    family: str,
    limit: int,
    corpus_snapshot_version: str | None = None,
    ranking_run_id: str | None = None,
    ranking_version: str | None = None,
) -> EvalComparePayload | None:
    if family not in VALID_FAMILIES:
        raise ValueError(f"Invalid recommendation family: {family!r}")

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        snap_default = latest_corpus_snapshot_version_with_works(conn)
        ctx = resolve_ranked_run_context(
            conn,
            ranking_run_id=ranking_run_id,
            corpus_snapshot_version=corpus_snapshot_version or snap_default,
            ranking_version=ranking_version,
        )
        if ctx is None:
            return None

        run_row = _fetch_ranking_run_row(conn, ranking_run_id=ctx.ranking_run_id)
        if run_row is None:
            return None

        config = _parse_config_row(run_row.get("config_json"))
        low_min, low_max = _low_cite_params_from_run_config(config)
        rev = None
        scope = config.get("selection_scope") if isinstance(config.get("selection_scope"), dict) else {}
        uc = scope.get("undercited") if isinstance(scope.get("undercited"), dict) else {}
        if isinstance(uc.get("low_cite_candidate_pool_revision"), str):
            rev = str(uc["low_cite_candidate_pool_revision"])

        pool_sql_t, pool_params = _pool_cte_sql(
            family=family,
            corpus_snapshot_version=ctx.corpus_snapshot_version,
            low_cite_min_year=low_min,
            low_cite_max_citations=low_max,
        )
        pool_size_row = conn.execute(pool_sql_t, pool_params).fetchone()
        pool_size = int(pool_size_row["pool_n"]) if pool_size_row else 0

        if family == "undercited":
            pool_def = (
                f"Low-cite candidate pool (revision {rev or 'v0'}): included core works in this corpus snapshot, "
                f"year≥{low_min}, citations≤{low_max}, non-empty title and abstract. "
                "Matches docs/candidate-pool-low-cite.md and the materialized undercited family scope."
            )
        else:
            pool_def = (
                "All included works in the corpus snapshot (same candidate set as the ranking run's emerging/bridge families)."
            )

        ranked_sql = """
        SELECT
            w.openalex_id,
            w.title,
            w.year,
            w.citation_count,
            w.source_slug,
            COALESCE(topic_agg.topics, '[]'::json) AS topics,
            ps.final_score
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
          AND ps.recommendation_family = %s
          AND w.corpus_snapshot_version = %s
          AND w.inclusion_status = 'included'
        """
        ranked_params: list[Any] = [ctx.ranking_run_id, family, ctx.corpus_snapshot_version]
        if family == "undercited":
            ranked_sql += """
          AND w.is_core_corpus = TRUE
          AND w.year >= %s
          AND w.citation_count <= %s
          AND length(trim(COALESCE(w.title, ''))) > 0
          AND length(trim(COALESCE(w.abstract, ''))) > 0
        """
            ranked_params.extend([low_min, low_max])
        ranked_sql += """
        ORDER BY ps.final_score DESC, ps.work_id ASC
        LIMIT %s
        """
        ranked_params.append(limit)
        ranked_rows = conn.execute(ranked_sql, tuple(ranked_params)).fetchall()

        cit_order = "pool.citation_count DESC, pool.year DESC, pool.openalex_id ASC"
        date_order = "pool.year DESC, pool.openalex_id ASC"

        cit_sql, cit_params = _select_from_pool(
            order_clause=cit_order,
            limit=limit,
            corpus_snapshot_version=ctx.corpus_snapshot_version,
            family=family,
            low_cite_min_year=low_min,
            low_cite_max_citations=low_max,
        )
        date_sql, date_params = _select_from_pool(
            order_clause=date_order,
            limit=limit,
            corpus_snapshot_version=ctx.corpus_snapshot_version,
            family=family,
            low_cite_min_year=low_min,
            low_cite_max_citations=low_max,
        )

        cit_rows = conn.execute(cit_sql, cit_params).fetchall()
        date_rows = conn.execute(date_sql, date_params).fetchall()

    ranked_items = [
        _row_to_eval_paper(dict(r), final_score=float(r["final_score"])) for r in ranked_rows
    ]
    cit_items = [_row_to_eval_paper(dict(r), final_score=None) for r in cit_rows]
    date_items = [_row_to_eval_paper(dict(r), final_score=None) for r in date_rows]

    ranked_arm = _arm_stats(
        ranked_items,
        arm_label="ranked_family",
        arm_desc=(
            "Materialized ranking run: order by final_score descending, then work_id (stable tie-break). "
            "Blend and signals follow this run's persisted family_weights and paper_scores (semantic may be used for Emerging when configured)."
        ),
        ordering_desc="final_score DESC, work_id ASC",
    )
    cit_arm = _arm_stats(
        cit_items,
        arm_label="citation_baseline",
        arm_desc="Popularity-style baseline on the same pool: highest citations first (not a relevance judgment).",
        ordering_desc="citation_count DESC, year DESC, openalex_id ASC",
    )
    date_arm = _arm_stats(
        date_items,
        arm_label="date_baseline",
        arm_desc="Pure recency baseline on the same pool: newest year first (not a relevance judgment).",
        ordering_desc="year DESC, openalex_id ASC",
    )

    sr = _topic_label_set(ranked_items)
    sc = _topic_label_set(cit_items)
    sd = _topic_label_set(date_items)
    overlap = EvalTopicOverlap(
        jaccard_ranked_vs_citation_baseline=round(_jaccard(sr, sc), 4),
        jaccard_ranked_vs_date_baseline=round(_jaccard(sr, sd), 4),
        jaccard_citation_vs_date_baseline=round(_jaccard(sc, sd), 4),
    )

    return EvalComparePayload(
        ranking_run_id=ctx.ranking_run_id,
        ranking_version=str(run_row["ranking_version"]),
        corpus_snapshot_version=ctx.corpus_snapshot_version,
        embedding_version=str(run_row["embedding_version"]),
        family=family,
        pool_definition=pool_def,
        pool_size=pool_size,
        low_cite_min_year=(low_min if family == "undercited" else None),
        low_cite_max_citations=(low_max if family == "undercited" else None),
        candidate_pool_doc_revision=(rev if family == "undercited" else None),
        ranked=ranked_arm,
        citation_baseline=cit_arm,
        date_baseline=date_arm,
        topic_overlap=overlap,
    )
