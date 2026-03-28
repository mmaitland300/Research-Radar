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
    utc_now,
)
from app.papers_repo import get_paper_detail as get_paper_detail_row
from app.papers_repo import list_papers

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
