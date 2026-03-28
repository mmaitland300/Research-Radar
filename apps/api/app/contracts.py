from datetime import datetime, timezone

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime


class ProductSummary(BaseModel):
    name: str
    thesis: str
    core_slice: list[str]
    edge_slice: list[str]
    pages: list[str]
    evaluation_checks: list[str]
    ranking_weights: dict[str, float]


class RankingFamily(BaseModel):
    key: str
    description: str


class EvaluationSummary(BaseModel):
    benchmark_target_size: str
    primary_metrics: list[str]
    checks: list[str]
    generated_at: datetime


class PaperDetail(BaseModel):
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


class PaperListItem(BaseModel):
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    source_label: str | None
    is_core_corpus: bool
    topics: list[str]


class PaperListResponse(BaseModel):
    total: int
    items: list[PaperListItem]


class UndercitedRecommendationItem(BaseModel):
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    reason: str
    signal_breakdown: dict[str, float]


class UndercitedRecommendationsResponse(BaseModel):
    """Heuristic v0 baseline, not a trained ranking model."""

    heuristic_label: str
    heuristic_version: str
    description: str
    total: int
    items: list[UndercitedRecommendationItem]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
