from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import psycopg
from psycopg.rows import dict_row

from app.papers_repo import _topic_names_from_json, database_url_from_env

SearchIncludedScope = Literal["core", "all_included"]
SearchFamilyHint = Literal["emerging", "bridge", "undercited"]

TITLE_VECTOR_SQL = "to_tsvector('english', coalesce(w.title, ''))"
ABSTRACT_VECTOR_SQL = "to_tsvector('english', coalesce(w.abstract, ''))"
DOCUMENT_VECTOR_SQL = (
    "setweight(to_tsvector('english', coalesce(w.title, '')), 'A') || "
    "setweight(to_tsvector('english', coalesce(w.abstract, '')), 'B')"
)

SEARCH_ORDERING = "lexical_rank desc, year desc, citation_count desc, work_id asc"


@dataclass(frozen=True)
class SearchResultRow:
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    source_label: str | None
    is_core_corpus: bool
    topics: list[str]
    preview: str | None
    matched_fields: list[str]
    highlight_fragments: list[str]
    lexical_rank: float


@dataclass(frozen=True)
class SearchResolvedFiltersRow:
    q: str
    limit: int
    offset: int
    year_from: int | None
    year_to: int | None
    included_scope: SearchIncludedScope
    source_slug: str | None
    topic: str | None
    family_hint: SearchFamilyHint | None


@dataclass(frozen=True)
class SearchResponseRow:
    total: int
    ordering: str
    resolved_filters: SearchResolvedFiltersRow
    items: list[SearchResultRow]


def _normalize_text_filter(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def _normalize_preview(value: str | None, *, max_len: int = 240) -> str | None:
    if value is None:
        return None
    compact = " ".join(value.split())
    if not compact:
        return None
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 1].rstrip() + "…"


def search_papers(
    *,
    q: str,
    limit: int,
    offset: int,
    year_from: int | None = None,
    year_to: int | None = None,
    included_scope: SearchIncludedScope = "all_included",
    source_slug: str | None = None,
    topic: str | None = None,
    family_hint: SearchFamilyHint | None = None,
) -> SearchResponseRow:
    resolved_q = _normalize_text_filter(q)
    resolved_source_slug = _normalize_text_filter(source_slug)
    resolved_topic = _normalize_text_filter(topic)
    if resolved_q is None:
        raise ValueError("q must not be empty.")
    if year_from is not None and year_to is not None and year_from > year_to:
        raise ValueError("year_from must be less than or equal to year_to.")

    where_clauses = [
        "w.inclusion_status = 'included'",
        f"{DOCUMENT_VECTOR_SQL} @@ st.ts_query",
    ]
    params: list[object] = [resolved_q]

    if included_scope == "core":
        where_clauses.append("w.is_core_corpus IS TRUE")
    if year_from is not None:
        where_clauses.append("w.year >= %s")
        params.append(year_from)
    if year_to is not None:
        where_clauses.append("w.year <= %s")
        params.append(year_to)
    if resolved_source_slug is not None:
        where_clauses.append("w.source_slug = %s")
        params.append(resolved_source_slug)
    if resolved_topic is not None:
        where_clauses.append(
            """
            EXISTS (
                SELECT 1
                FROM work_topics wt_filter
                JOIN topics t_filter ON t_filter.id = wt_filter.topic_id
                WHERE wt_filter.work_id = w.id
                  AND t_filter.name ILIKE %s
            )
            """
        )
        params.append(f"%{resolved_topic}%")
    if family_hint is not None:
        where_clauses.append(
            """
            EXISTS (
                SELECT 1
                FROM paper_scores ps_filter
                JOIN ranking_runs rr_filter
                  ON rr_filter.ranking_run_id = ps_filter.ranking_run_id
                WHERE ps_filter.work_id = w.id
                  AND ps_filter.recommendation_family = %s
                  AND rr_filter.status = 'succeeded'
                  AND rr_filter.corpus_snapshot_version = w.corpus_snapshot_version
                  AND rr_filter.ranking_run_id = (
                      SELECT rr_latest.ranking_run_id
                      FROM ranking_runs rr_latest
                      WHERE rr_latest.status = 'succeeded'
                        AND rr_latest.corpus_snapshot_version = w.corpus_snapshot_version
                      ORDER BY rr_latest.finished_at DESC NULLS LAST, rr_latest.started_at DESC
                      LIMIT 1
                  )
            )
            """
        )
        params.append(family_hint)

    where_sql = "\n          AND ".join(where_clauses)
    params.extend([limit, offset])

    query = f"""
        WITH search_terms AS (
            SELECT websearch_to_tsquery('english', %s) AS ts_query
        )
        SELECT
            w.id AS work_id,
            w.openalex_id,
            w.title,
            w.year,
            w.citation_count,
            w.source_slug,
            v.display_name AS source_label,
            w.is_core_corpus,
            COALESCE(topic_agg.topics, '[]'::json) AS topics,
            NULLIF(trim(coalesce(w.abstract, '')), '') AS abstract_preview,
            ARRAY_REMOVE(
                ARRAY[
                    CASE WHEN {TITLE_VECTOR_SQL} @@ st.ts_query THEN 'title' END,
                    CASE WHEN {ABSTRACT_VECTOR_SQL} @@ st.ts_query THEN 'abstract' END
                ],
                NULL
            ) AS matched_fields,
            ARRAY_REMOVE(
                ARRAY[
                    CASE
                        WHEN {TITLE_VECTOR_SQL} @@ st.ts_query
                        THEN ts_headline(
                            'english',
                            w.title,
                            st.ts_query,
                            'StartSel=[[ , StopSel= ]], MaxWords=12, MinWords=3'
                        )
                    END,
                    CASE
                        WHEN {ABSTRACT_VECTOR_SQL} @@ st.ts_query
                        THEN ts_headline(
                            'english',
                            coalesce(w.abstract, ''),
                            st.ts_query,
                            'StartSel=[[ , StopSel= ]], MaxFragments=2, MaxWords=16, MinWords=6, FragmentDelimiter= … '
                        )
                    END
                ],
                NULL
            ) AS highlight_fragments,
            ts_rank_cd({DOCUMENT_VECTOR_SQL}, st.ts_query, 32) AS lexical_rank,
            count(*) OVER() AS total_count
        FROM works w
        CROSS JOIN search_terms st
        LEFT JOIN venues v ON v.id = w.venue_id
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
        WHERE {where_sql}
        ORDER BY lexical_rank DESC, w.year DESC, w.citation_count DESC, w.id ASC
        LIMIT %s OFFSET %s
    """

    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        rows = conn.execute(query, params).fetchall()

    items = [
        SearchResultRow(
            paper_id=str(row["openalex_id"]),
            title=str(row["title"]),
            year=int(row["year"]),
            citation_count=int(row["citation_count"] or 0),
            source_slug=str(row["source_slug"]) if row["source_slug"] is not None else None,
            source_label=str(row["source_label"]) if row["source_label"] is not None else None,
            is_core_corpus=bool(row["is_core_corpus"]),
            topics=_topic_names_from_json(row["topics"]),
            preview=_normalize_preview(str(row["abstract_preview"])) if row["abstract_preview"] is not None else None,
            matched_fields=[str(x) for x in (row["matched_fields"] or []) if x],
            highlight_fragments=[str(x) for x in (row["highlight_fragments"] or []) if x],
            lexical_rank=round(float(row["lexical_rank"]), 6),
        )
        for row in rows
    ]
    total = int(rows[0]["total_count"]) if rows else 0

    return SearchResponseRow(
        total=total,
        ordering=SEARCH_ORDERING,
        resolved_filters=SearchResolvedFiltersRow(
            q=resolved_q,
            limit=limit,
            offset=offset,
            year_from=year_from,
            year_to=year_to,
            included_scope=included_scope,
            source_slug=resolved_source_slug,
            topic=resolved_topic,
            family_hint=family_hint,
        ),
        items=items,
    )
