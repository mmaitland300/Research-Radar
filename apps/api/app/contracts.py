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
    summary: str
    recommendation_family: str
    ranking_version: str
    signal_breakdown: dict[str, float]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
