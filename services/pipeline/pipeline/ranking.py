from __future__ import annotations

from dataclasses import dataclass
from math import fsum
from typing import Any

LOW_CITE_CANDIDATE_POOL_DOC = "docs/candidate-pool-low-cite.md"
LOW_CITE_CANDIDATE_POOL_REVISION = "v0"
DEFAULT_LOW_CITE_MIN_YEAR = 2019
DEFAULT_LOW_CITE_MAX_CITATIONS = 30


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


@dataclass(frozen=True)
class RankingCandidate:
    work_id: int
    year: int
    citation_count: int
    topic_ids: tuple[int, ...] = ()
    is_core_corpus: bool = True
    title: str = ""
    abstract: str | None = None


def in_low_cite_candidate_pool(
    candidate: RankingCandidate,
    *,
    min_year: int = DEFAULT_LOW_CITE_MIN_YEAR,
    max_citations: int = DEFAULT_LOW_CITE_MAX_CITATIONS,
) -> bool:
    """
    Frozen definition in LOW_CITE_CANDIDATE_POOL_DOC (revision LOW_CITE_CANDIDATE_POOL_REVISION).
    Used for the undercited recommendation family only; emerging/bridge use the full included set.
    """
    if not candidate.is_core_corpus:
        return False
    if candidate.year < min_year:
        return False
    if candidate.citation_count > max_citations:
        return False
    if not str(candidate.title or "").strip():
        return False
    if not str(candidate.abstract or "").strip():
        return False
    return True


@dataclass(frozen=True)
class PaperScoreRow:
    """One persisted row in paper_scores (per work, family, run)."""

    work_id: int
    recommendation_family: str
    semantic_score: float | None
    citation_velocity_score: float | None
    topic_growth_score: float | None
    bridge_score: float | None
    diversity_penalty: float | None
    final_score: float
    reason_short: str
    bridge_eligible: bool | None = None
    bridge_signal_json: dict[str, Any] | None = None


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


def final_score_partial(
    *,
    semantic: float | None,
    citation_velocity: float | None,
    topic_growth: float | None,
    bridge: float | None,
    diversity_penalty: float | None,
    weights: ScoreWeights | None = None,
) -> float:
    """
    Combine available positive signals with ScoreWeights, renormalizing weights over
    non-null semantic/citation_velocity/topic_growth/bridge terms only.
    diversity_penalty applies when not None; otherwise treated as 0.
    """
    active = weights or ScoreWeights()
    terms: list[tuple[float, float]] = []
    if semantic is not None:
        terms.append((active.semantic, semantic))
    if citation_velocity is not None:
        terms.append((active.citation_velocity, citation_velocity))
    if topic_growth is not None:
        terms.append((active.topic_growth, topic_growth))
    if bridge is not None:
        terms.append((active.bridge, bridge))

    if not terms:
        positive = 0.0
    else:
        w_sum = fsum(w for w, _ in terms)
        if w_sum <= 0:
            positive = 0.0
        else:
            positive = fsum((w / w_sum) * v for w, v in terms)

    dp = diversity_penalty if diversity_penalty is not None else 0.0
    return positive - (active.diversity_penalty * dp)