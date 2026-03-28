from fastapi import FastAPI, HTTPException, Query

from app.config import settings
from app.contracts import (
    EvaluationSummary,
    HealthResponse,
    PaperDetail,
    PaperListItem,
    PaperListResponse,
    ProductSummary,
    RankingFamily,
    UndercitedRecommendationItem,
    UndercitedRecommendationsResponse,
    utc_now,
)
from app.papers_repo import get_paper_detail as get_paper_detail_row
from app.papers_repo import list_papers
from app.papers_repo import list_undercited_heuristic_v0

app = FastAPI(
    title="Research Radar API",
    version="0.1.0",
    description="API surface for ranking, explainability, and evaluation in the Research Radar project.",
)


@app.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", timestamp=utc_now())


@app.get("/api/v1/meta/product", response_model=ProductSummary)
def get_product_summary() -> ProductSummary:
    return ProductSummary(
        name=settings.name,
        thesis=settings.thesis,
        core_slice=list(settings.core_slice),
        edge_slice=list(settings.edge_slice),
        pages=list(settings.v1_pages),
        evaluation_checks=list(settings.evaluation_checks),
        ranking_weights={
            "semantic": settings.weights.semantic,
            "citation_velocity": settings.weights.citation_velocity,
            "topic_growth": settings.weights.topic_growth,
            "bridge": settings.weights.bridge,
            "diversity_penalty": settings.weights.diversity_penalty,
        },
    )


@app.get(
    "/api/v1/recommendations/undercited",
    response_model=UndercitedRecommendationsResponse,
)
def get_recommendations_undercited(
    limit: int = Query(default=15, ge=1, le=100),
    min_year: int = Query(default=2019, ge=1990, le=2100),
    max_citations: int = Query(default=30, ge=0, le=10_000),
) -> UndercitedRecommendationsResponse:
    """
    Heuristic v0 baseline: recent core-corpus papers with low citations and basic metadata quality.
    Not a trained ranking model.
    """
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
            "Rule-based baseline: included core papers since min_year with citation count "
            "at or below max_citations, non-empty title and abstract. Order: newest year first, "
            "then fewer citations first."
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


@app.get("/api/v1/recommendations/families", response_model=list[RankingFamily])
def get_recommendation_families() -> list[RankingFamily]:
    descriptions = {
        "emerging": "High-growth, semantically relevant work before it becomes consensus canon.",
        "bridge": "Work connecting nearby but distinct local clusters.",
        "undercited": "Relevant work that beats popularity-only ranking baselines.",
    }
    return [
        RankingFamily(key=family, description=descriptions[family])
        for family in settings.recommendation_families
    ]


@app.get("/api/v1/evaluation/summary", response_model=EvaluationSummary)
def get_evaluation_summary() -> EvaluationSummary:
    return EvaluationSummary(
        benchmark_target_size="100-200 papers",
        primary_metrics=["precision@10", "precision@20"],
        checks=list(settings.evaluation_checks),
        generated_at=utc_now(),
    )


@app.get("/api/v1/papers/{paper_id}", response_model=PaperDetail)
def get_paper_detail(paper_id: str) -> PaperDetail:
    try:
        paper = get_paper_detail_row(paper_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database query failed. Confirm Postgres is running and seeded.") from exc

    if paper is None:
        raise HTTPException(status_code=404, detail="Paper not found.")

    return PaperDetail(
        paper_id=paper.paper_id,
        title=paper.title,
        abstract=paper.abstract,
        venue=paper.venue,
        year=paper.year,
        citation_count=paper.citation_count,
        source_slug=paper.source_slug,
        is_core_corpus=paper.is_core_corpus,
        authors=paper.authors,
        topics=paper.topics,
    )


@app.get("/api/v1/papers", response_model=PaperListResponse)
def get_papers(
    q: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
) -> PaperListResponse:
    try:
        papers = list_papers(limit=limit, q=q)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database query failed. Confirm Postgres is running and seeded.") from exc

    return PaperListResponse(
        total=len(papers),
        items=[
            PaperListItem(
                paper_id=paper.paper_id,
                title=paper.title,
                year=paper.year,
                citation_count=paper.citation_count,
                source_slug=paper.source_slug,
                is_core_corpus=paper.is_core_corpus,
            )
            for paper in papers
        ],
    )
