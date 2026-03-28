import os
from dataclasses import dataclass
from datetime import date

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


UNDERCIITED_HEURISTIC_V0_REASON = (
    "Recent core paper with low citation count (heuristic v0 baseline)."
)


@dataclass(frozen=True)
class UndercitedHeuristicRow:
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    reason: str
    signal_breakdown: dict[str, float]


def _heuristic_v0_breakdown(
    *,
    year: int,
    citation_count: int,
    min_year: int,
    max_citations: int,
) -> dict[str, float]:
    """Rule-based scores in [0, 1] for transparency only; not a learned ranker."""
    today = date.today()
    current_year = today.year
    span = max(1, current_year - min_year + 1)
    recency = (year - min_year + 1) / span
    recency = max(0.0, min(1.0, recency))
    cap = max(1, max_citations)
    low_cite = 1.0 - min(1.0, float(citation_count) / float(cap + 1))
    core_gate = 1.0
    metadata_gate = 1.0
    composite = 0.45 * recency + 0.55 * low_cite
    return {
        "recency": round(recency, 4),
        "low_citation_signal": round(low_cite, 4),
        "core_corpus_gate": core_gate,
        "metadata_quality_gate": metadata_gate,
        "heuristic_composite": round(composite, 4),
    }


def list_undercited_heuristic_v0(
    *,
    limit: int,
    min_year: int,
    max_citations: int,
) -> list[UndercitedHeuristicRow]:
    """
    Newest included core papers with citation count at or below a ceiling,
    with a minimum metadata gate (non-empty title and abstract).
    """
    query = """
        SELECT
            openalex_id,
            title,
            year,
            citation_count,
            source_slug
        FROM works
        WHERE inclusion_status = 'included'
          AND is_core_corpus = TRUE
          AND year >= %s
          AND citation_count <= %s
          AND length(trim(COALESCE(title, ''))) > 0
          AND length(trim(COALESCE(abstract, ''))) > 0
        ORDER BY year DESC, citation_count ASC, openalex_id ASC
        LIMIT %s
    """
    params: list[object] = [min_year, max_citations, limit]

    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        rows = conn.execute(query, params).fetchall()

    out: list[UndercitedHeuristicRow] = []
    for row in rows:
        y = int(row["year"])
        c = int(row["citation_count"] or 0)
        breakdown = _heuristic_v0_breakdown(
            year=y,
            citation_count=c,
            min_year=min_year,
            max_citations=max_citations,
        )
        out.append(
            UndercitedHeuristicRow(
                paper_id=str(row["openalex_id"]),
                title=str(row["title"]),
                year=y,
                citation_count=c,
                source_slug=str(row["source_slug"]) if row["source_slug"] is not None else None,
                reason=UNDERCIITED_HEURISTIC_V0_REASON,
                signal_breakdown=breakdown,
            )
        )
    return out
