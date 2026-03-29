"""Read-only similar-papers retrieval from stored embeddings (pgvector); no live embedding in API."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row

from app.papers_repo import _topic_names_from_json, database_url_from_env


@dataclass(frozen=True)
class SimilarPaperRow:
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    topics: list[str]
    similarity: float


@dataclass(frozen=True)
class SimilarPapersResult:
    """Source paper (must have an embedding row) and its nearest neighbors by cosine similarity."""

    paper_id: str
    embedding_version: str
    items: list[SimilarPaperRow]


def _source_has_embedding(
    conn: psycopg.Connection, *, paper_id: str, embedding_version: str
) -> str | None:
    """Return canonical openalex_id if included work has embedding for version; else None."""
    row = conn.execute(
        """
        SELECT w.openalex_id
        FROM works w
        INNER JOIN embeddings e
          ON e.work_id = w.id AND e.embedding_version = %s
        WHERE w.openalex_id = %s
          AND w.inclusion_status = 'included'
        LIMIT 1
        """,
        (embedding_version, paper_id),
    ).fetchone()
    if row is None:
        return None
    return str(row["openalex_id"])


_NEIGHBORS_SQL = """
WITH target AS (
    SELECT w.id AS id, e.vector AS vector
    FROM works w
    INNER JOIN embeddings e
      ON e.work_id = w.id AND e.embedding_version = %s
    WHERE w.openalex_id = %s
      AND w.inclusion_status = 'included'
)
SELECT
    w2.openalex_id,
    w2.title,
    w2.year,
    w2.citation_count,
    w2.source_slug,
    COALESCE(topic_agg.topics, '[]'::json) AS topics,
    1 - (e2.vector <=> t.vector) AS similarity
FROM target t
INNER JOIN embeddings e2
  ON e2.embedding_version = %s
INNER JOIN works w2
  ON w2.id = e2.work_id
LEFT JOIN LATERAL (
    SELECT json_agg(sub.topic_name ORDER BY sub.score DESC, sub.topic_name ASC) AS topics
    FROM (
        SELECT t.name AS topic_name, wt.score AS score
        FROM work_topics wt
        JOIN topics t ON t.id = wt.topic_id
        WHERE wt.work_id = w2.id
        ORDER BY wt.score DESC, t.name ASC
        LIMIT 3
    ) sub
) topic_agg ON TRUE
WHERE w2.id <> t.id
  AND w2.inclusion_status = 'included'
ORDER BY e2.vector <=> t.vector ASC
LIMIT %s
"""


def list_similar_papers(
    *,
    paper_id: str,
    embedding_version: str,
    limit: int,
) -> SimilarPapersResult | None:
    """
    Nearest included neighbors by cosine distance on stored vectors.
    Returns None if the source work is missing, not included, or has no row in embeddings
    for embedding_version.
    """
    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        canonical_id = _source_has_embedding(
            conn, paper_id=paper_id, embedding_version=embedding_version
        )
        if canonical_id is None:
            return None
        rows = conn.execute(
            _NEIGHBORS_SQL,
            (embedding_version, paper_id, embedding_version, limit),
        ).fetchall()

    items = [
        SimilarPaperRow(
            paper_id=str(row["openalex_id"]),
            title=str(row["title"]),
            year=int(row["year"]),
            citation_count=int(row["citation_count"] or 0),
            source_slug=str(row["source_slug"]) if row["source_slug"] is not None else None,
            topics=_topic_names_from_json(row["topics"]),
            similarity=round(float(row["similarity"]), 4),
        )
        for row in rows
    ]
    return SimilarPapersResult(
        paper_id=canonical_id,
        embedding_version=embedding_version,
        items=items,
    )
