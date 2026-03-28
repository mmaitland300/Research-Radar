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
