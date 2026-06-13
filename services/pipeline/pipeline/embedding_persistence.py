from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Sequence

import psycopg

from pipeline.snapshot_membership import count_included_memberships, latest_snapshot_with_included_memberships


@dataclass(frozen=True)
class EmbeddingCandidate:
    work_id: int
    title: str
    abstract: str | None


def latest_corpus_snapshot_version_with_works(conn: psycopg.Connection) -> str | None:
    """Backward-compatible alias; resolves via snapshot membership rows."""
    return latest_snapshot_with_included_memberships(conn)


def count_included_works_for_snapshot(conn: psycopg.Connection, corpus_snapshot_version: str) -> int:
    return count_included_memberships(conn, snapshot_version=corpus_snapshot_version)


def count_missing_embedding_candidates(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    embedding_version: str,
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM works w
        JOIN work_source_snapshot_memberships wssm
          ON wssm.work_id = w.id
         AND wssm.source_snapshot_version = %s
         AND wssm.inclusion_status = 'included'
        LEFT JOIN embeddings e
          ON e.work_id = w.id
         AND e.embedding_version = %s
        WHERE e.work_id IS NULL
        """,
        (corpus_snapshot_version, embedding_version),
    ).fetchone()
    return int(row[0] or 0) if row is not None else 0


def list_embedding_candidates(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    embedding_version: str,
    limit: int | None = None,
) -> list[EmbeddingCandidate]:
    sql = """
        SELECT w.id, w.title, w.abstract
        FROM works w
        JOIN work_source_snapshot_memberships wssm
          ON wssm.work_id = w.id
         AND wssm.source_snapshot_version = %s
         AND wssm.inclusion_status = 'included'
        LEFT JOIN embeddings e
          ON e.work_id = w.id
         AND e.embedding_version = %s
        WHERE e.work_id IS NULL
        ORDER BY w.id ASC
    """
    params: tuple[object, ...] = (corpus_snapshot_version, embedding_version)
    if limit is not None:
        sql += "\n        LIMIT %s"
        params = (corpus_snapshot_version, embedding_version, limit)

    rows = conn.execute(sql, params).fetchall()
    return [
        EmbeddingCandidate(
            work_id=int(row[0]),
            title=str(row[1]),
            abstract=str(row[2]) if row[2] is not None else None,
        )
        for row in rows
    ]


def _vector_literal(vector: Sequence[float]) -> str:
    if not vector:
        raise ValueError("Embedding vector must not be empty.")
    normalized: list[float] = []
    for value in vector:
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("Embedding vector values must be finite numbers.")
        normalized.append(number)
    return json.dumps(normalized, separators=(",", ":"))


def upsert_work_embeddings(
    conn: psycopg.Connection,
    *,
    embedding_version: str,
    rows: Sequence[tuple[int, Sequence[float]]],
) -> None:
    for work_id, vector in rows:
        conn.execute(
            """
            INSERT INTO embeddings (work_id, embedding_version, vector)
            VALUES (%s, %s, %s::vector)
            ON CONFLICT (work_id, embedding_version) DO UPDATE SET
                vector = EXCLUDED.vector
            """,
            (work_id, embedding_version, _vector_literal(vector)),
        )
