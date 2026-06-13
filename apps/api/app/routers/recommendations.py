from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from app.config import settings
from app.contracts import (
    RankedRecommendationsResponse,
    RankingFamily,
    UndercitedRecommendationItem,
    UndercitedRecommendationsResponse,
)
from app.demo_fixtures import (
    fixture_mode_enabled,
    fixture_ranked_recommendations,
    fixture_undercited_recommendations,
)
from app.ml_bridge_scorer_rollout import maybe_build_bridge_scorer_ranked_response
from app.ml_scorer_rollout import (
    build_ranked_recommendations_response,
    get_canary_subject,
    maybe_build_scorer_ranked_response,
)
from app.papers_repo import list_undercited_heuristic_v0
from app.scores_repo import list_ranked_recommendations

router = APIRouter()


@router.get(
    "/api/v1/recommendations/undercited",
    response_model=UndercitedRecommendationsResponse,
)
def get_recommendations_undercited(
    limit: int = Query(default=15, ge=1, le=100),
    min_year: int = Query(default=2019, ge=1990, le=2100),
    max_citations: int = Query(default=30, ge=0, le=10_000),
) -> UndercitedRecommendationsResponse:
    """
    Heuristic v0 baseline: frozen low-cite candidate pool (docs/candidate-pool-low-cite.md v0) —
    included core papers, recency and citation ceiling, non-empty title and abstract.
    Global query (not corpus-snapshot scoped). For snapshot-scoped comparisons, use
    GET /api/v1/evaluation/compare?family=undercited. Not a trained ranking model.
    """
    if fixture_mode_enabled():
        return fixture_undercited_recommendations(
            limit=limit,
            min_year=min_year,
            max_citations=max_citations,
        )
    try:
        rows = list_undercited_heuristic_v0(
            limit=limit,
            min_year=min_year,
            max_citations=max_citations,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and seeded.",
        ) from exc

    return UndercitedRecommendationsResponse(
        heuristic_label="undercited-core-recent-v0",
        heuristic_version="v0",
        description=(
            "Frozen low-cite candidate pool (docs/candidate-pool-low-cite.md v0): included core papers, "
            "recency and citation ceiling, non-empty title and abstract. Global listing (not snapshot-scoped). "
            "Order: year DESC, citation_count ASC, openalex_id ASC."
        ),
        total=len(rows),
        items=[
            UndercitedRecommendationItem(
                paper_id=r.paper_id,
                title=r.title,
                year=r.year,
                citation_count=r.citation_count,
                source_slug=r.source_slug,
                reason=r.reason,
                signal_breakdown=r.signal_breakdown,
            )
            for r in rows
        ],
    )


@router.get(
    "/api/v1/recommendations/ranked",
    response_model=RankedRecommendationsResponse,
    response_model_exclude_none=False,
)
def get_recommendations_ranked(
    request: Request,
    family: Literal["emerging", "bridge", "undercited"] = Query(...),
    limit: int = Query(default=20, ge=1, le=100),
    corpus_snapshot_version: str | None = Query(default=None),
    ranking_run_id: str | None = Query(default=None),
    ranking_version: str | None = Query(default=None),
    bridge_eligible_only: bool = Query(
        default=False,
        description=(
            "If true, return only bridge rows with bridge_eligible = true (SQL: IS TRUE). "
            "Only applies when family=bridge; for other families this parameter is ignored. "
            "Rows with false or null eligibility are excluded (null is legacy or unset neighbor_mix)."
        ),
    ),
) -> RankedRecommendationsResponse:
    """
    Read persisted paper_scores for a succeeded ranking run (latest for snapshot unless
    ranking_run_id or ranking_version narrows the choice).
    """
    if fixture_mode_enabled():
        return fixture_ranked_recommendations(
            family=family,
            limit=limit,
            bridge_eligible_only=bridge_eligible_only,
        )
    try:
        subject = get_canary_subject(request)
        if family == "bridge":
            scorer_response = maybe_build_bridge_scorer_ranked_response(
                family=family,
                limit=limit,
                corpus_snapshot_version=corpus_snapshot_version,
                ranking_run_id=ranking_run_id,
                ranking_version=ranking_version,
                bridge_eligible_only=bridge_eligible_only,
                subject=subject,
            )
        else:
            scorer_response = maybe_build_scorer_ranked_response(
                family=family,
                limit=limit,
                corpus_snapshot_version=corpus_snapshot_version,
                ranking_run_id=ranking_run_id,
                ranking_version=ranking_version,
                bridge_eligible_only=bridge_eligible_only,
                subject=subject,
            )
        if scorer_response is not None:
            return scorer_response

        resolved = list_ranked_recommendations(
            family=family,
            limit=limit,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
            bridge_eligible_only=bridge_eligible_only,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and ranking data exists.",
        ) from exc

    if resolved is None:
        raise HTTPException(
            status_code=404,
            detail="No succeeded ranking run found for the given filters.",
        )

    ctx, rows, run_config = resolved
    return build_ranked_recommendations_response(ctx, rows, run_config, family)


@router.get("/api/v1/recommendations/families", response_model=list[RankingFamily])
def get_recommendation_families() -> list[RankingFamily]:
    descriptions = {
        "emerging": "High-growth work in the curated corpus, ordered by the selected materialized ranking run.",
        "bridge": "Work intended to connect nearby but distinct corpus neighborhoods; bridge eligibility is run-dependent.",
        "undercited": "Low-cite candidate-pool work surfaced by the selected materialized ranking run.",
    }
    return [
        RankingFamily(key=family, description=descriptions[family])
        for family in settings.recommendation_families
    ]
