from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.config import RankingCounts, RankingRun
from pipeline.ranking import PaperScoreRow, RankingCandidate


def _topic_ids_tuple(value: Any) -> tuple[int, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(int(item) for item in value if item is not None)
    return ()


def latest_corpus_snapshot_version_with_works(conn: psycopg.Connection) -> str | None:
    """
    Latest source_snapshot_version (by snapshot created_at) that has at least one
    included work referencing that corpus_snapshot_version on works.
    """
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
    return str(row[0])


def list_ranking_candidates(
    conn: psycopg.Connection, corpus_snapshot_version: str
) -> list[RankingCandidate]:
    rows = conn.execute(
        """
        SELECT
            w.id,
            w.year,
            w.citation_count,
            w.is_core_corpus,
            w.title,
            w.abstract,
            COALESCE(
                array_agg(wt.topic_id ORDER BY wt.score DESC) FILTER (WHERE wt.topic_id IS NOT NULL),
                '{}'::bigint[]
            ) AS topic_ids
        FROM works w
        LEFT JOIN work_topics wt ON wt.work_id = w.id
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
        GROUP BY w.id, w.year, w.citation_count, w.is_core_corpus, w.title, w.abstract
        ORDER BY w.id ASC
        """,
        (corpus_snapshot_version,),
    ).fetchall()
    return [
        RankingCandidate(
            work_id=int(row[0]),
            year=int(row[1]),
            citation_count=int(row[2] or 0),
            is_core_corpus=bool(row[3]),
            title=str(row[4] or ""),
            abstract=str(row[5]) if row[5] is not None else None,
            topic_ids=_topic_ids_tuple(row[6]),
        )
        for row in rows
    ]


def latest_successful_ranking_run_id(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    ranking_version: str | None = None,
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
    return str(row[0])


def insert_ranking_run_started(conn: psycopg.Connection, run: RankingRun) -> None:
    conn.execute(
        """
        INSERT INTO ranking_runs (
            ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version,
            status, started_at, finished_at, config_json, counts_json, error_message, notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, NULL, %s::jsonb, NULL, NULL, %s)
        """,
        (
            run.ranking_run_id,
            run.ranking_version,
            run.corpus_snapshot_version,
            run.embedding_version,
            run.status,
            run.started_at,
            json.dumps(run.config),
            run.notes,
        ),
    )


def update_ranking_run_final(
    conn: psycopg.Connection,
    ranking_run_id: str,
    status: str,
    counts: RankingCounts | None,
    error_message: str | None,
) -> None:
    finished_at = datetime.now(UTC)
    counts_json = json.dumps(asdict(counts), default=str) if counts is not None else None
    conn.execute(
        """
        UPDATE ranking_runs
        SET status = %s, finished_at = %s, counts_json = %s::jsonb, error_message = %s
        WHERE ranking_run_id = %s
        """,
        (status, finished_at, counts_json, error_message, ranking_run_id),
    )


def upsert_paper_scores(conn: psycopg.Connection, ranking_run_id: str, rows: Sequence[PaperScoreRow]) -> None:
    for r in rows:
        conn.execute(
            """
            INSERT INTO paper_scores (
                ranking_run_id, work_id, recommendation_family,
                semantic_score, citation_velocity_score, topic_growth_score,
                bridge_score, bridge_eligible, bridge_signal_json,
                diversity_penalty, final_score, reason_short
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT (ranking_run_id, work_id, recommendation_family) DO UPDATE SET
                semantic_score = EXCLUDED.semantic_score,
                citation_velocity_score = EXCLUDED.citation_velocity_score,
                topic_growth_score = EXCLUDED.topic_growth_score,
                bridge_score = EXCLUDED.bridge_score,
                bridge_eligible = EXCLUDED.bridge_eligible,
                bridge_signal_json = EXCLUDED.bridge_signal_json,
                diversity_penalty = EXCLUDED.diversity_penalty,
                final_score = EXCLUDED.final_score,
                reason_short = EXCLUDED.reason_short
            """,
            (
                ranking_run_id,
                r.work_id,
                r.recommendation_family,
                r.semantic_score,
                r.citation_velocity_score,
                r.topic_growth_score,
                r.bridge_score,
                r.bridge_eligible,
                json.dumps(r.bridge_signal_json) if r.bridge_signal_json is not None else None,
                r.diversity_penalty,
                r.final_score,
                r.reason_short,
            ),
        )


def fetch_paper_scores_for_run(
    conn: psycopg.Connection, ranking_run_id: str
) -> list[dict[str, Any]]:
    """Read back scores (for tests / future API)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT work_id, recommendation_family, semantic_score, citation_velocity_score,
                   topic_growth_score, bridge_score, bridge_eligible, bridge_signal_json,
                   diversity_penalty, final_score, reason_short
            FROM paper_scores
            WHERE ranking_run_id = %s
            ORDER BY work_id, recommendation_family
            """,
            (ranking_run_id,),
        )
        return [dict(row) for row in cur.fetchall()]