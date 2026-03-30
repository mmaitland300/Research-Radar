from dataclasses import dataclass, field


@dataclass(frozen=True)
class RankingWeights:
    semantic: float = 0.30
    citation_velocity: float = 0.20
    topic_growth: float = 0.20
    bridge: float = 0.20
    diversity_penalty: float = 0.10


@dataclass(frozen=True)
class ProductConfig:
    name: str = "Research Radar"
    thesis: str = "Find emerging and bridge papers in audio ML before they become default citations."
    core_slice: tuple[str, ...] = ("MIR", "audio representation learning")
    edge_slice: tuple[str, ...] = ("neural audio effects", "music/audio generation")
    v1_pages: tuple[str, ...] = (
        "search",
        "recommended",
        "paper-detail",
        "trends",
        "evaluation",
    )
    evaluation_checks: tuple[str, ...] = (
        "hand-reviewed relevance benchmark",
        "novelty/diversity vs citation baseline",
        "freeze-at-T temporal backtest",
    )
    recommendation_families: tuple[str, ...] = ("emerging", "bridge", "undercited")
    weights: RankingWeights = field(default_factory=RankingWeights)


settings = ProductConfig()

PRODUCT_RANKING_METADATA_NOTE = (
    "ranking_weights are illustrative long-term formula defaults (semantic, citation_velocity, "
    "topic_growth, bridge, diversity_penalty) from the product build brief. They do not match "
    "per-family weights used when writing paper_scores; those live in ranking_runs.config_json "
    "for each materialized run. When materialized_ranking is present, treat its config_json as "
    "the source of truth for the latest succeeded run on the default corpus snapshot."
)
