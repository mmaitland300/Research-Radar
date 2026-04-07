"""Read path for materialized ranking runs and paper_scores (no pipeline package dependency)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.papers_repo import _topic_names_from_json, database_url_from_env

VALID_FAMILIES: frozenset[str] = frozenset({"emerging", "bridge", "undercited"})


@dataclass(frozen=True)
class RankedRecommendationRow:
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    topics: list[str]
    semantic_score: float | None
    citation_velocity_score: float | None
    topic_growth_score: float | None
    bridge_score: float | None
    diversity_penalty: float | None
    final_score: float
    reason_short: str


@dataclass(frozen=True)
class RankedRunContext:
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str


@dataclass(frozen=True)
class MaterializedRankingForMeta:
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    config_json: dict[str, Any]


def latest_corpus_snapshot_version_with_works(conn: psycopg.Connection) -> str | None:
    row = conn.execute(
        """
        SELECT ssv.source_snapshot_version
        FROM source_snapshot_versions ssv
        WHERE EXISTS (
            SELECT 1
            FROM works w
            WHERE w.corpus_snapshot_version = ssv.source_snapshot_version
              AND w.inclusion_status = 'included'
        )
        ORDER BY ssv.created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return str(row["source_snapshot_version"])


def fetch_latest_materialized_ranking_for_meta() -> MaterializedRankingForMeta | None:
    """
    Latest succeeded ranking run for the newest corpus snapshot that has included works,
    same default resolution as GET /api/v1/recommendations/ranked without filters.
    """
    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        snap = latest_corpus_snapshot_version_with_works(conn)
        if snap is None:
            return None
        rid = _latest_successful_ranking_run_id(
            conn, corpus_snapshot_version=snap, ranking_version=None
        )
        if rid is None:
            return None
        row = conn.execute(
            """
            SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, config_json
            FROM ranking_runs
            WHERE ranking_run_id = %s AND status = 'succeeded'
            """,
            (rid,),
        ).fetchone()
        if row is None:
            return None
        raw_cfg = row["config_json"]
        if isinstance(raw_cfg, str):
            parsed = json.loads(raw_cfg)
            cfg: dict[str, Any] = parsed if isinstance(parsed, dict) else {}
        elif isinstance(raw_cfg, dict):
            cfg = dict(raw_cfg)
        else:
            cfg = {}
        return MaterializedRankingForMeta(
            ranking_run_id=str(row["ranking_run_id"]),
            ranking_version=str(row["ranking_version"]),
            corpus_snapshot_version=str(row["corpus_snapshot_version"]),
            embedding_version=str(row["embedding_version"]),
            config_json=cfg,
        )


def _latest_successful_ranking_run_id(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    ranking_version: str | None,
) -> str | None:
    if ranking_version is None:
        row = conn.execute(
            """
            SELECT ranking_run_id
            FROM ranking_runs
            WHERE corpus_snapshot_version = %s
              AND status = 'succeeded'
            ORDER BY finished_at DESC NULLS LAST, started_at DESC
            LIMIT 1
            """,
            (corpus_snapshot_version,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT ranking_run_id
            FROM ranking_runs
            WHERE corpus_snapshot_version = %s
              AND ranking_version = %s
              AND status = 'succeeded'
            ORDER BY finished_at DESC NULLS LAST, started_at DESC
            LIMIT 1
            """,
            (corpus_snapshot_version, ranking_version),
        ).fetchone()
    if row is None:
        return None
    return str(row["ranking_run_id"])


def resolve_ranked_run_context(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str | None,
    corpus_snapshot_version: str | None,
    ranking_version: str | None,
) -> RankedRunContext | None:
    """
    Pick a succeeded run: explicit ranking_run_id wins; else latest succeeded for
    corpus snapshot (default = latest snapshot with included works) and optional ranking_version.
    """
    if ranking_run_id:
        row = conn.execute(
            """
            SELECT ranking_run_id, ranking_version, corpus_snapshot_version
            FROM ranking_runs
            WHERE ranking_run_id = %s AND status = 'succeeded'
            """,
            (ranking_run_id,),
        ).fetchone()
        if row is None:
            return None
        return RankedRunContext(
            ranking_run_id=str(row["ranking_run_id"]),
            ranking_version=str(row["ranking_version"]),
            corpus_snapshot_version=str(row["corpus_snapshot_version"]),
        )

    snap = corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
    if snap is None:
        return None
    rid = _latest_successful_ranking_run_id(
        conn, corpus_snapshot_version=snap, ranking_version=ranking_version
    )
    if rid is None:
        return None
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version
        FROM ranking_runs
        WHERE ranking_run_id = %s AND status = 'succeeded'
        """,
        (rid,),
    ).fetchone()
    if row is None:
        return None
    return RankedRunContext(
        ranking_run_id=str(row["ranking_run_id"]),
        ranking_version=str(row["ranking_version"]),
        corpus_snapshot_version=str(row["corpus_snapshot_version"]),
    )


def _parse_config_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    if isinstance(raw, dict):
        return dict(raw)
    return {}


def list_ranked_recommendations(
    *,
    family: str,
    limit: int,
    corpus_snapshot_version: str | None = None,
    ranking_run_id: str | None = None,
    ranking_version: str | None = None,
) -> tuple[RankedRunContext, list[RankedRecommendationRow], dict[str, Any]] | None:
    if family not in VALID_FAMILIES:
        raise ValueError(f"Invalid recommendation family: {family!r}")

    query = """
        SELECT
            w.openalex_id,
            w.title,
            w.year,
            w.citation_count,
            w.source_slug,
            COALESCE(topic_agg.topics, '[]'::json) AS topics,
            ps.semantic_score,
            ps.citation_velocity_score,
            ps.topic_growth_score,
            ps.bridge_score,
            ps.diversity_penalty,
            ps.final_score,
            ps.reason_short
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
        ORDER BY ps.final_score DESC
        LIMIT %s
    """
    params: tuple[Any, ...]

    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        ctx = resolve_ranked_run_context(
            conn,
            ranking_run_id=ranking_run_id,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_version=ranking_version,
        )
        if ctx is None:
            return None
        params = (ctx.ranking_run_id, family, limit)
        rows = conn.execute(query, params).fetchall()
        cfg_row = conn.execute(
            """
            SELECT config_json
            FROM ranking_runs
            WHERE ranking_run_id = %s
            """,
            (ctx.ranking_run_id,),
        ).fetchone()
        run_config = _parse_config_json(cfg_row["config_json"] if cfg_row else None)

    items = [
        RankedRecommendationRow(
            paper_id=str(row["openalex_id"]),
            title=str(row["title"]),
            year=int(row["year"]),
            citation_count=int(row["citation_count"] or 0),
            source_slug=str(row["source_slug"]) if row["source_slug"] is not None else None,
            topics=_topic_names_from_json(row["topics"]),
            semantic_score=float(row["semantic_score"]) if row["semantic_score"] is not None else None,
            citation_velocity_score=float(row["citation_velocity_score"])
            if row["citation_velocity_score"] is not None
            else None,
            topic_growth_score=float(row["topic_growth_score"])
            if row["topic_growth_score"] is not None
            else None,
            bridge_score=float(row["bridge_score"]) if row["bridge_score"] is not None else None,
            diversity_penalty=float(row["diversity_penalty"])
            if row["diversity_penalty"] is not None
            else None,
            final_score=float(row["final_score"]),
            reason_short=str(row["reason_short"]),
        )
        for row in rows
    ]
    return ctx, items, run_config
