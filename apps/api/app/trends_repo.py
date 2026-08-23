from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row

from app.papers_repo import database_url_from_env
from app.serving_context import resolve_serving_context


@dataclass(frozen=True)
class TopicTrendRow:
    topic_id: int
    topic_name: str
    total_works: int
    recent_works: int
    prior_works: int
    delta: int
    growth_label: str


@dataclass(frozen=True)
class TopicTrendsResult:
    corpus_snapshot_version: str
    rows: list[TopicTrendRow]


def _growth_label(*, recent_works: int, prior_works: int) -> str:
    if recent_works > prior_works:
        return "rising"
    if recent_works < prior_works:
        return "cooling"
    return "steady"


def list_topic_trends(
    *,
    limit: int,
    since_year: int,
    min_works: int,
    corpus_snapshot_version: str | None = None,
    ranking_run_id: str | None = None,
    ranking_version: str | None = None,
) -> TopicTrendsResult:
    query = """
        SELECT
            t.id AS topic_id,
            t.name AS topic_name,
            COUNT(DISTINCT w.id) AS total_works,
            COUNT(DISTINCT w.id) FILTER (WHERE w.year >= %s) AS recent_works,
            COUNT(DISTINCT w.id) FILTER (WHERE w.year < %s) AS prior_works
        FROM topics t
        JOIN work_topics wt ON wt.topic_id = t.id
        JOIN works w ON w.id = wt.work_id
        JOIN work_source_snapshot_memberships wssm
          ON wssm.work_id = w.id
         AND wssm.source_snapshot_version = %s
         AND wssm.inclusion_status = 'included'
        GROUP BY t.id, t.name
        HAVING COUNT(DISTINCT w.id) >= %s
        ORDER BY
            COUNT(DISTINCT w.id) FILTER (WHERE w.year >= %s) DESC,
            (
                COUNT(DISTINCT w.id) FILTER (WHERE w.year >= %s)
                - COUNT(DISTINCT w.id) FILTER (WHERE w.year < %s)
            ) DESC,
            COUNT(DISTINCT w.id) DESC,
            t.name ASC
        LIMIT %s
    """
    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        ctx = resolve_serving_context(
            conn,
            ranking_run_id=ranking_run_id,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_version=ranking_version,
        )
        resolved_snapshot = ctx.corpus_snapshot_version
        params = (
            since_year,
            since_year,
            resolved_snapshot,
            min_works,
            since_year,
            since_year,
            since_year,
            limit,
        )
        rows = conn.execute(query, params).fetchall()

    return TopicTrendsResult(
        corpus_snapshot_version=resolved_snapshot,
        rows=[
            TopicTrendRow(
                topic_id=int(row["topic_id"]),
                topic_name=str(row["topic_name"]),
                total_works=int(row["total_works"] or 0),
                recent_works=int(row["recent_works"] or 0),
                prior_works=int(row["prior_works"] or 0),
                delta=int(row["recent_works"] or 0) - int(row["prior_works"] or 0),
                growth_label=_growth_label(
                    recent_works=int(row["recent_works"] or 0),
                    prior_works=int(row["prior_works"] or 0),
                ),
            )
            for row in rows
        ],
    )
