from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Sequence

import psycopg


@dataclass(frozen=True)
class EmbeddingCandidate:
    work_id: int
    title: str
    abstract: str | None


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
    return str(row[0])


def count_included_works_for_snapshot(conn: psycopg.Connection, corpus_snapshot_version: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM works w
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
        """,
        (corpus_snapshot_version,),
    ).fetchone()
    return int(row[0] or 0) if row is not None else 0


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
        LEFT JOIN embeddings e
          ON e.work_id = w.id
         AND e.embedding_version = %s
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
          AND e.work_id IS NULL
        """,
        (embedding_version, corpus_snapshot_version),
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
        LEFT JOIN embeddings e
          ON e.work_id = w.id
         AND e.embedding_version = %s
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
          AND e.work_id IS NULL
        ORDER BY w.id ASC
    """
    params: tuple[object, ...] = (embedding_version, corpus_snapshot_version)
    if limit is not None:
        sql += "\n        LIMIT %s"
        params = (embedding_version, corpus_snapshot_version, limit)

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
