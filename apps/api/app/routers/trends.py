from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.contracts import TopicTrendItem, TopicTrendsResponse, utc_now
from app.demo_fixtures import fixture_mode_enabled, fixture_topic_trends
from app.serving_context import ServingContextNotFoundError, ServingContextUnavailableError
from app.trends_repo import list_topic_trends

router = APIRouter()


@router.get("/api/v1/trends/topics", response_model=TopicTrendsResponse)
def get_topic_trends(
    limit: int = Query(default=20, ge=1, le=100),
    since_year: int = Query(default=utc_now().year - 1, ge=1990, le=2100),
    min_works: int = Query(default=2, ge=1, le=10_000),
    corpus_snapshot_version: str | None = Query(default=None),
    ranking_run_id: str | None = Query(default=None),
    ranking_version: str | None = Query(default=None),
) -> TopicTrendsResponse:
    if fixture_mode_enabled():
        return fixture_topic_trends(
            limit=limit,
            since_year=since_year,
            min_works=min_works,
        )
    try:
        result = list_topic_trends(
            limit=limit,
            since_year=since_year,
            min_works=min_works,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
        )
    except ServingContextNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ServingContextUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and topic data exists.",
        ) from exc

    return TopicTrendsResponse(
        corpus_snapshot_version=result.corpus_snapshot_version,
        since_year=since_year,
        min_works=min_works,
        total=len(result.rows),
        items=[
            TopicTrendItem(
                topic_id=r.topic_id,
                topic_name=r.topic_name,
                total_works=r.total_works,
                recent_works=r.recent_works,
                prior_works=r.prior_works,
                delta=r.delta,
                growth_label=r.growth_label,
            )
            for r in result.rows
        ],
        generated_at=utc_now(),
    )
