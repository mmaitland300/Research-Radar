import os
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row


@dataclass(frozen=True)
class PaperRow:
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    is_core_corpus: bool


@dataclass(frozen=True)
class PaperDetailRow:
    paper_id: str
    title: str
    abstract: str
    venue: str | None
    year: int
    citation_count: int
    source_slug: str | None
    is_core_corpus: bool
    authors: list[str]
    topics: list[str]


def database_url_from_env() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "research_radar")
    password = os.environ.get("PGPASSWORD", "research_radar")
    db = os.environ.get("PGDATABASE", "research_radar")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def list_papers(limit: int, q: str | None = None) -> list[PaperRow]:
    query = """
        SELECT
            openalex_id,
            title,
            year,
            citation_count,
            source_slug,
            is_core_corpus
        FROM works
        WHERE inclusion_status = 'included'
    """
    params: list[object] = []
    if q:
        query += " AND title ILIKE %s"
        params.append(f"%{q}%")

    query += " ORDER BY year DESC, citation_count DESC LIMIT %s"
    params.append(limit)

    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        rows = conn.execute(query, params).fetchall()

    return [
        PaperRow(
            paper_id=str(row["openalex_id"]),
            title=str(row["title"]),
            year=int(row["year"]),
            citation_count=int(row["citation_count"] or 0),
            source_slug=str(row["source_slug"]) if row["source_slug"] is not None else None,
            is_core_corpus=bool(row["is_core_corpus"]),
        )
        for row in rows
    ]


def get_paper_detail(paper_id: str) -> PaperDetailRow | None:
    query = """
        SELECT
            w.id AS work_id,
            w.openalex_id,
            w.title,
            COALESCE(w.abstract, '') AS abstract,
            v.display_name AS venue_name,
            w.year,
            w.citation_count,
            w.source_slug,
            w.is_core_corpus,
            COALESCE(
                (
                    SELECT json_agg(a.display_name ORDER BY wa.author_position)
                    FROM work_authors wa
                    JOIN authors a ON a.id = wa.author_id
                    WHERE wa.work_id = w.id
                ),
                '[]'::json
            ) AS authors,
            COALESCE(
                (
                    SELECT json_agg(t.name ORDER BY wt.score DESC)
                    FROM work_topics wt
                    JOIN topics t ON t.id = wt.topic_id
                    WHERE wt.work_id = w.id
                ),
                '[]'::json
            ) AS topics
        FROM works w
        LEFT JOIN venues v ON v.id = w.venue_id
        WHERE w.openalex_id = %s
          AND w.inclusion_status = 'included'
        LIMIT 1
    """
    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        row = conn.execute(query, [paper_id]).fetchone()

    if row is None:
        return None

    authors = row["authors"] if isinstance(row["authors"], list) else []
    topics = row["topics"] if isinstance(row["topics"], list) else []

    return PaperDetailRow(
        paper_id=str(row["openalex_id"]),
        title=str(row["title"]),
        abstract=str(row["abstract"] or ""),
        venue=str(row["venue_name"]) if row["venue_name"] is not None else None,
        year=int(row["year"]),
        citation_count=int(row["citation_count"] or 0),
        source_slug=str(row["source_slug"]) if row["source_slug"] is not None else None,
        is_core_corpus=bool(row["is_core_corpus"]),
        authors=[str(item) for item in authors if item],
        topics=[str(item) for item in topics if item],
    )
