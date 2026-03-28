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
