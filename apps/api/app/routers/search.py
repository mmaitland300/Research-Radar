from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app.contracts import (
    SearchMatchMetadata,
    SearchResolvedFilters,
    SearchResponse,
    SearchResultItem,
)
from app.demo_fixtures import fixture_mode_enabled, fixture_search
from app.search_repo import SearchRunContextNotFoundError, search_papers

router = APIRouter()


@router.get(
    "/api/v1/search",
    response_model=SearchResponse,
    response_model_exclude_none=True,
)
def get_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=15, ge=1, le=100),
    offset: int = Query(default=0, ge=0, le=10_000),
    year_from: int | None = Query(default=None, ge=1900, le=2100),
    year_to: int | None = Query(default=None, ge=1900, le=2100),
    included_scope: Literal["core", "all_included"] = Query(default="all_included"),
    source_slug: str | None = Query(default=None, min_length=1),
    topic: str | None = Query(default=None, min_length=1),
    family_hint: Literal["emerging", "bridge", "undercited"] | None = Query(default=None),
    ranking_run_id: str | None = Query(default=None),
    ranking_version: str | None = Query(default=None),
) -> SearchResponse:
    if fixture_mode_enabled():
        return fixture_search(
            q=q,
            limit=limit,
            offset=offset,
            year_from=year_from,
            year_to=year_to,
            included_scope=included_scope,
            source_slug=source_slug,
            topic=topic,
            family_hint=family_hint,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
        )
    try:
        payload = search_papers(
            q=q,
            limit=limit,
            offset=offset,
            year_from=year_from,
            year_to=year_to,
            included_scope=included_scope,
            source_slug=source_slug,
            topic=topic,
            family_hint=family_hint,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SearchRunContextNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and search data exists.",
        ) from exc

    return SearchResponse(
        total=payload.total,
        ordering=payload.ordering,
        resolved_filters=SearchResolvedFilters(
            q=payload.resolved_filters.q,
            limit=payload.resolved_filters.limit,
            offset=payload.resolved_filters.offset,
            year_from=payload.resolved_filters.year_from,
            year_to=payload.resolved_filters.year_to,
            included_scope=payload.resolved_filters.included_scope,
            source_slug=payload.resolved_filters.source_slug,
            topic=payload.resolved_filters.topic,
            family_hint=payload.resolved_filters.family_hint,
            ranking_run_id=payload.resolved_filters.ranking_run_id,
            ranking_version=payload.resolved_filters.ranking_version,
        ),
        items=[
            SearchResultItem(
                paper_id=item.paper_id,
                title=item.title,
                year=item.year,
                citation_count=item.citation_count,
                source_slug=item.source_slug,
                source_label=item.source_label,
                is_core_corpus=item.is_core_corpus,
                topics=item.topics,
                preview=item.preview,
                match=SearchMatchMetadata(
                    matched_fields=item.matched_fields,
                    highlight_fragments=item.highlight_fragments,
                    lexical_rank=item.lexical_rank,
                ),
            )
            for item in payload.items
        ],
        resolved_ranking_run_id=payload.resolved_ranking_run_id,
        resolved_ranking_version=payload.resolved_ranking_version,
        resolved_corpus_snapshot_version=payload.resolved_corpus_snapshot_version,
    )
