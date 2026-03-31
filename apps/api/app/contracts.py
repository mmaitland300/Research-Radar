from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime


class ReadinessResponse(BaseModel):
    """Liveness stays cheap; readiness includes a database dependency check."""

    status: str
    database: str
    timestamp: datetime


class EvaluationDisclaimer(BaseModel):
    headline: str
    bullets: list[str]


class EvaluationPaperItem(BaseModel):
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None = None
    topics: list[str]
    final_score: float | None = None


class EvaluationRecencyProxy(BaseModel):
    mean_year: float
    min_year: int
    max_year: int
    share_in_latest_two_years: float


class EvaluationCitationProxy(BaseModel):
    mean: float
    median: float
    min_val: int
    max_val: int


class EvaluationTopicMixProxy(BaseModel):
    unique_topic_labels: int
    top_topics: list[str]


class EvaluationListArmResponse(BaseModel):
    arm_label: str
    arm_description: str
    ordering_description: str
    items: list[EvaluationPaperItem]
    recency: EvaluationRecencyProxy
    citations: EvaluationCitationProxy
    topics: EvaluationTopicMixProxy


class EvaluationTopicOverlap(BaseModel):
    jaccard_ranked_vs_citation_baseline: float
    jaccard_ranked_vs_date_baseline: float
    jaccard_citation_vs_date_baseline: float


class EvaluationCompareResponse(BaseModel):
    disclaimer: EvaluationDisclaimer
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    family: str
    pool_definition: str
    pool_size: int
    low_cite_min_year: int | None = None
    low_cite_max_citations: int | None = None
    candidate_pool_doc_revision: str | None = None
    topic_overlap_note: str
    ranked: EvaluationListArmResponse
    citation_baseline: EvaluationListArmResponse
    date_baseline: EvaluationListArmResponse
    topic_overlap: EvaluationTopicOverlap
    generated_at: datetime


class MaterializedRankingMeta(BaseModel):
    """Latest succeeded ranking run on the default corpus snapshot (for transparency)."""

    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    config_json: dict[str, Any]


class ProductSummary(BaseModel):
    name: str
    thesis: str
    core_slice: list[str]
    edge_slice: list[str]
    pages: list[str]
    evaluation_checks: list[str]
    ranking_weights: dict[str, float]
    ranking_metadata_note: str
    materialized_ranking: MaterializedRankingMeta | None = None


class RankingFamily(BaseModel):
    key: str
    description: str


class EvaluationSummary(BaseModel):
    benchmark_target_size: str
    primary_metrics: list[str]
    checks: list[str]
    generated_at: datetime


class TopicTrendItem(BaseModel):
    topic_id: int
    topic_name: str
    total_works: int
    recent_works: int
    prior_works: int
    delta: int
    growth_label: str


class TopicTrendsResponse(BaseModel):
    corpus_snapshot_version: str
    since_year: int
    min_works: int
    total: int
    items: list[TopicTrendItem]
    generated_at: datetime


class ClusterSamplePaperItem(BaseModel):
    paper_id: str
    title: str


class ClusterGroupItem(BaseModel):
    cluster_id: str
    work_count: int
    sample_papers: list[ClusterSamplePaperItem]


class ClusterInspectionResponse(BaseModel):
    cluster_version: str
    embedding_version: str
    corpus_snapshot_version: str
    algorithm: str
    status: str
    clustering_metric: str | None = None
    metric_note: str | None = None
    groups: list[ClusterGroupItem]
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
    """Heuristic v0: frozen low-cite pool (docs/candidate-pool-low-cite.md); global, not snapshot-scoped."""

    heuristic_label: str
    heuristic_version: str
    description: str
    total: int
    items: list[UndercitedRecommendationItem]


class RankedSignals(BaseModel):
    """Nullable components from paper_scores; semantic/bridge may be unset until embeddings exist."""

    semantic: float | None = None
    citation_velocity: float | None = None
    topic_growth: float | None = None
    bridge: float | None = None
    diversity_penalty: float | None = None


class RankedRecommendationItem(BaseModel):
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None = None
    topics: list[str]
    signals: RankedSignals
    final_score: float
    reason_short: str


class RankedRecommendationsResponse(BaseModel):
    """Materialized ranking run: one row per (work, family) from pipeline ranking-run."""

    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    family: str
    total: int
    items: list[RankedRecommendationItem]


class SimilarPaperItem(BaseModel):
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None = None
    topics: list[str]
    similarity: float


class SimilarPapersResponse(BaseModel):
    """Nearest neighbors from stored embeddings only (same embedding_version as source)."""

    paper_id: str
    embedding_version: str
    total: int
    items: list[SimilarPaperItem]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
