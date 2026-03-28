from dataclasses import dataclass
from math import fsum


@dataclass(frozen=True)
class ScoreWeights:
    semantic: float = 0.30
    citation_velocity: float = 0.20
    topic_growth: float = 0.20
    bridge: float = 0.20
    diversity_penalty: float = 0.10


@dataclass(frozen=True)
class PaperSignals:
    semantic: float
    citation_velocity: float
    topic_growth: float
    bridge: float
    diversity_penalty: float


def final_score(signals: PaperSignals, weights: ScoreWeights | None = None) -> float:
    active = weights or ScoreWeights()
    positive = fsum(
        [
            active.semantic * signals.semantic,
            active.citation_velocity * signals.citation_velocity,
            active.topic_growth * signals.topic_growth,
            active.bridge * signals.bridge,
        ]
    )
    return positive - (active.diversity_penalty * signals.diversity_penalty)
