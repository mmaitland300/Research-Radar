from fastapi import FastAPI

from app.config import settings
from app.contracts import (
    EvaluationSummary,
    HealthResponse,
    PaperDetail,
    ProductSummary,
    RankingFamily,
    utc_now,
)

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
    return PaperDetail(
        paper_id=paper_id,
        title="Placeholder bridge-paper example",
        summary="This placeholder response exists so the scaffold includes the paper-detail product surface from day one.",
        recommendation_family="bridge",
        ranking_version="ranking-v0",
        signal_breakdown={
            "semantic": 0.84,
            "citation_velocity": 0.52,
            "topic_growth": 0.67,
            "bridge": 0.91,
            "diversity_penalty": 0.18,
            "final": 0.68,
        },
    )
