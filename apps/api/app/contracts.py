from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


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


BridgeDistinctnessNextStep = Literal[
    "inspect_cluster_quality_first",
    "eligible_filter_not_distinct_enough",
    "candidate_for_small_weight_experiment",
    "insufficient_bridge_signal_coverage",
]


class BridgeDistinctnessOverlapMetrics(BaseModel):
    overlap_count: int
    jaccard: float


class BridgeDistinctnessDecisionSupport(BaseModel):
    """Heuristic only; does not validate bridge ranking or end-user relevance."""

    eligible_head_differs_from_full: bool
    eligible_head_less_emerging_like_than_full: bool
    suggested_next_step: BridgeDistinctnessNextStep


class BridgeDistinctnessResponse(BaseModel):
    """Pinned-run structural comparison of bridge heads vs emerging; diagnostics only."""

    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    cluster_version: str | None = None
    k: int
    full_bridge_top_k_ids: list[str]
    eligible_bridge_top_k_ids: list[str]
    emerging_top_k_ids: list[str]
    full_bridge_vs_eligible_bridge: BridgeDistinctnessOverlapMetrics
    full_bridge_vs_emerging: BridgeDistinctnessOverlapMetrics
    eligible_bridge_vs_emerging: BridgeDistinctnessOverlapMetrics
    bridge_family_row_count: int
    bridge_score_nonnull_count: int
    bridge_score_null_count: int
    bridge_eligible_true_count: int
    bridge_eligible_false_count: int
    bridge_eligible_null_count: int
    bridge_signal_json_present_count: int
    bridge_signal_json_missing_count: int
    decision_support: BridgeDistinctnessDecisionSupport
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
    current_evaluation_type: str = Field(
        ...,
        description=(
            "What the product reports today: proxy / corpus metrics vs baselines, "
            "not a human-judged relevance P@k benchmark."
        ),
    )
    is_human_labeled_benchmark_current: bool = Field(
        ...,
        description=(
            "True only if human labels are in use to compute the metrics below. "
            "V0 is False; see planned_labeled_benchmark for roadmap targets only."
        ),
    )
    planned_labeled_benchmark: dict[str, str | list[str]] = Field(
        ...,
        description=(
            "Intended *future* labeled set (not current measurements): corpus size and "
            "P@k-style metrics to run when a gold set exists."
        ),
    )
    benchmark_target_size: str = Field(
        ...,
        description=(
            "Same roadmap target as planned_labeled_benchmark[corpus] (legacy key; not a current benchmark result). "
            "Use is_human_labeled_benchmark_current to interpret."
        ),
    )
    primary_metrics: list[str] = Field(
        ...,
        description=(
            "Planned metrics for a future human-labeled evaluation (legacy key; not current reported P@k). "
            "Use is_human_labeled_benchmark_current to interpret."
        ),
    )
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


SearchIncludedScope = Literal["core", "all_included"]
SearchFamilyHint = Literal["emerging", "bridge", "undercited"]


class SearchMatchMetadata(BaseModel):
    matched_fields: list[str]
    highlight_fragments: list[str]
    lexical_rank: float


class SearchResultItem(BaseModel):
    paper_id: str
    title: str
    year: int
    citation_count: int
    source_slug: str | None
    source_label: str | None
    is_core_corpus: bool
    topics: list[str]
    preview: str | None = None
    match: SearchMatchMetadata


class SearchResolvedFilters(BaseModel):
    q: str
    limit: int
    offset: int
    year_from: int | None = None
    year_to: int | None = None
    included_scope: SearchIncludedScope
    source_slug: str | None = None
    topic: str | None = None
    family_hint: SearchFamilyHint | None = None
    ranking_run_id: str | None = Field(
        default=None,
        description=(
            "Normalized search query filter. Only present when family_hint made search depend on "
            "ranking state and the caller supplied ranking_run_id."
        ),
    )
    ranking_version: str | None = Field(
        default=None,
        description=(
            "Normalized search query filter. Only present when family_hint made search depend on "
            "ranking state and the caller supplied ranking_version."
        ),
    )


class SearchResponse(BaseModel):
    total: int
    ordering: str
    resolved_filters: SearchResolvedFilters
    items: list[SearchResultItem]
    resolved_ranking_run_id: str | None = Field(
        default=None,
        description=(
            "Resolved succeeded ranking run used for family-filtered search. Omitted when search "
            "was lexical-only and did not depend on ranking state."
        ),
    )
    resolved_ranking_version: str | None = Field(
        default=None,
        description=(
            "Resolved ranking_version used for family-filtered search. Omitted when search was "
            "lexical-only and did not depend on ranking state."
        ),
    )
    resolved_corpus_snapshot_version: str | None = Field(
        default=None,
        description=(
            "Corpus snapshot version implied by the resolved ranking run. Omitted when search was "
            "lexical-only and did not depend on ranking state."
        ),
    )


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


RankedSignalRole = Literal["used", "measured", "experimental", "penalty", "not_computed"]


class RankedSignalExplanation(BaseModel):
    """Per-signal derived explanation aligned with the same weights as final_score for this run."""

    key: str
    label: str
    role: RankedSignalRole
    value: float | None = None
    contribution: float | None = None
    summary: str


class RankedListExplanation(BaseModel):
    """Family-level copy for how the list is ordered (weights from ranking run config when present)."""

    family: str
    headline: str
    bullets: list[str]
    used_in_ordering: list[str]
    measured_only: list[str]
    experimental: list[str]


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
    signal_explanations: list[RankedSignalExplanation]
    bridge_eligible: bool | None = Field(
        default=None,
        description=(
            "neighbor_mix_v1 gate for bridge-family rows only. On runs that persist neighbor_mix_v1: "
            "true when eligible, false when ineligible or when mix support is missing for that work "
            "(e.g. not in clustering inputs). Null only for non-bridge families or legacy bridge rows "
            "from runs that never wrote neighbor_mix eligibility."
        ),
    )


class RankedRecommendationsResponse(BaseModel):
    """Materialized ranking run: one row per (work, family) from pipeline ranking-run."""

    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    family: str
    total: int
    list_explanation: RankedListExplanation
    items: list[RankedRecommendationItem]


class PaperRankingFamilyItem(BaseModel):
    family: str
    present: bool
    in_top_n: bool
    rank: int | None = None
    final_score: float | None = None
    reason_short: str | None = None
    signals: RankedSignals | None = None
    signal_explanations: list[RankedSignalExplanation] = Field(default_factory=list)
    bridge_eligible: bool | None = Field(
        default=None,
        description=(
            "Same meaning as RankedRecommendationItem.bridge_eligible. Null for non-bridge families, "
            "legacy bridge rows, or families with no materialized row for the paper."
        ),
    )


class PaperRankingResponse(BaseModel):
    paper_id: str
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    top_n: int
    rank_scope: str
    families: list[PaperRankingFamilyItem]


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
