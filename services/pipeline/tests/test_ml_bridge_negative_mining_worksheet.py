"""Tests for bridge negative-mining worksheet selection."""

from __future__ import annotations

from pipeline.ml_bridge_negative_mining_worksheet import (
    ALLOWED_SAMPLE_REASONS,
    BridgeMiningCandidate,
    select_bridge_negative_sample,
    stable_row_id,
)


def _cand(
    rank: int,
    *,
    paper_id: str,
    final_score: float,
    bridge_score: float | None = None,
    bridge_eligible: bool | None = None,
) -> BridgeMiningCandidate:
    token = paper_id.rsplit("/", 1)[-1]
    return BridgeMiningCandidate(
        family_rank=rank,
        paper_id=paper_id,
        work_token=token,
        internal_work_id=rank,
        title=f"Paper {rank}",
        year=2025,
        citation_count=1,
        source_slug="tismir",
        topics_raw='["Music and Audio Processing"]',
        abstract="Music retrieval abstract.",
        final_score=final_score,
        semantic_score=None,
        citation_velocity_score=0.1,
        topic_growth_score=1.0,
        bridge_score=bridge_score,
        diversity_penalty=0.0,
        bridge_eligible=bridge_eligible,
        reason_short="test",
    )


def test_stable_row_id_is_deterministic() -> None:
    a = stable_row_id(
        worksheet_version="ml-bridge-negative-mining-v1",
        sample_seed=20260531,
        paper_id="https://openalex.org/W123",
    )
    b = stable_row_id(
        worksheet_version="ml-bridge-negative-mining-v1",
        sample_seed=20260531,
        paper_id="https://openalex.org/W123",
    )
    assert a == b
    assert len(a) == 64


def test_select_bridge_negative_sample_prefers_deep_and_suppressed_buckets() -> None:
    candidates = []
    for rank in range(1, 101):
        fs = 0.7 - (rank * 0.01)
        bridge = 0.95 if rank > 20 else 0.5
        candidates.append(
            _cand(
                rank,
                paper_id=f"https://openalex.org/W{rank:04d}",
                final_score=fs,
                bridge_score=bridge,
                bridge_eligible=False if rank >= 90 else True,
            )
        )

    selected, debug = select_bridge_negative_sample(candidates, total_rows=20, seed=20260531)
    assert len(selected) == 20
    assert debug["achieved_rows"] == 20
    reasons = {item.sample_reason for item in selected}
    assert "bridge_deep_cut" in reasons
    assert reasons.issubset(set(ALLOWED_SAMPLE_REASONS))
    ranks = [item.candidate.family_rank for item in selected]
    assert min(ranks) >= 21
