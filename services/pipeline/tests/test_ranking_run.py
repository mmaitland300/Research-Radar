from pipeline.bridge_neighbor_mix import NEIGHBOR_MIX_V1_DEFAULT_K, NeighborMixV1Result
from pipeline.config import RankingCounts, RankingRun
from pipeline.ranking import RankingCandidate
import pytest

from pipeline.ranking_run import (
    BRIDGE_REASON_LEGACY,
    BRIDGE_REASON_NO_CLUSTER,
    BRIDGE_REASON_STRUCTURAL,
    BRIDGE_REASON_STRUCTURAL_WEIGHTED,
    BRIDGE_REASON_STRUCTURAL_ZERO_WEIGHT,
    RECOMMENDATION_FAMILIES,
    _build_ranking_config,
    _ranking_counts_from_rows,
    build_step3_heuristic_score_rows,
    resolved_family_weights,
    validate_bridge_weight_for_bridge_family,
)


def _pool_candidate(
    *,
    work_id: int,
    year: int = 2026,
    citation_count: int = 5,
    topic_ids: tuple[int, ...] = (1,),
    is_core_corpus: bool = True,
    title: str = "Title",
    abstract: str | None = "Abstract text",
) -> RankingCandidate:
    return RankingCandidate(
        work_id=work_id,
        year=year,
        citation_count=citation_count,
        topic_ids=topic_ids,
        is_core_corpus=is_core_corpus,
        title=title,
        abstract=abstract,
    )


def test_ranking_run_start_complete_fail_lifecycle() -> None:
    run = RankingRun.start(
        ranking_version="v0-test",
        corpus_snapshot_version="source-snapshot-test",
        embedding_version="none-v0",
        config={"k": "v"},
        notes="n",
    )
    assert run.status == "running"
    assert run.ranking_run_id.startswith("rank-")
    counts = RankingCounts(
        total_candidate_works=1,
        total_rows_written=3,
        rows_by_family={"emerging": 1, "bridge": 1, "undercited": 1},
        rows_null_semantic=3,
        rows_null_bridge=3,
    )
    done = run.complete(counts)
    assert done.status == "succeeded"
    assert done.finished_at is not None
    assert done.error_message is None

    failed = run.fail("boom")
    assert failed.status == "failed"
    assert failed.error_message == "boom"


def test_build_step3_heuristic_score_rows_shape_and_family_differences() -> None:
    candidates = [
        _pool_candidate(work_id=10, topic_ids=(1, 2, 3), citation_count=8),
        _pool_candidate(work_id=20, topic_ids=(1,), citation_count=1),
        _pool_candidate(work_id=30, topic_ids=(2,), year=2024, citation_count=20),
    ]

    rows = build_step3_heuristic_score_rows(candidates)

    assert len(rows) == 9
    for row in rows:
        assert row.recommendation_family in RECOMMENDATION_FAMILIES
        assert row.semantic_score is None
        assert row.bridge_score is None
        assert row.bridge_eligible is None
        assert row.bridge_signal_json is None
        assert row.reason_short
        assert isinstance(row.final_score, float)

    by_key = {(row.work_id, row.recommendation_family): row for row in rows}
    assert by_key[(10, "bridge")].final_score > by_key[(20, "bridge")].final_score
    assert by_key[(20, "undercited")].final_score > by_key[(30, "undercited")].final_score
    assert by_key[(10, "emerging")].final_score > by_key[(30, "emerging")].final_score


def test_undercited_rows_only_for_low_cite_pool_members() -> None:
    in_pool = _pool_candidate(work_id=1, citation_count=5)
    high_cite = _pool_candidate(work_id=2, citation_count=100)
    edge_not_core = _pool_candidate(work_id=3, is_core_corpus=False)
    no_abstract = _pool_candidate(work_id=4, abstract="   ")

    rows = build_step3_heuristic_score_rows([in_pool, high_cite, edge_not_core, no_abstract])
    under = [r for r in rows if r.recommendation_family == "undercited"]
    assert len(under) == 1
    assert under[0].work_id == 1
    assert len([r for r in rows if r.recommendation_family == "emerging"]) == 4
    assert len([r for r in rows if r.recommendation_family == "bridge"]) == 4


def test_citation_popularity_penalty_normalized_within_pool_only() -> None:
    pool_a = _pool_candidate(work_id=1, citation_count=0)
    pool_b = _pool_candidate(work_id=2, citation_count=10)
    outside = _pool_candidate(work_id=3, citation_count=999, title="x", abstract="y")
    rows = build_step3_heuristic_score_rows([pool_a, pool_b, outside])
    u1 = next(r for r in rows if r.work_id == 1 and r.recommendation_family == "undercited")
    u2 = next(r for r in rows if r.work_id == 2 and r.recommendation_family == "undercited")
    assert u1.final_score > u2.final_score


def test_bridge_weight_override_changes_bridge_final_score_not_emerging() -> None:
    candidates = [_pool_candidate(work_id=10, topic_ids=(1, 2, 3), citation_count=8)]
    bridge_map = {10: 0.8}
    rows0 = build_step3_heuristic_score_rows(
        candidates,
        cluster_version="cv",
        bridge_boundary_by_work=bridge_map,
        bridge_weight_for_bridge_family=0.0,
    )
    rows_w = build_step3_heuristic_score_rows(
        candidates,
        cluster_version="cv",
        bridge_boundary_by_work=bridge_map,
        bridge_weight_for_bridge_family=0.15,
    )
    e0 = next(r for r in rows0 if r.recommendation_family == "emerging")
    e1 = next(r for r in rows_w if r.recommendation_family == "emerging")
    assert e0.final_score == e1.final_score
    b0 = next(r for r in rows0 if r.recommendation_family == "bridge")
    b1 = next(r for r in rows_w if r.recommendation_family == "bridge")
    assert b1.final_score != b0.final_score
    assert b0.reason_short == BRIDGE_REASON_STRUCTURAL
    assert b1.reason_short == BRIDGE_REASON_STRUCTURAL_WEIGHTED


def test_validate_bridge_weight_for_bridge_family_range() -> None:
    assert validate_bridge_weight_for_bridge_family(0.0) == 0.0
    assert validate_bridge_weight_for_bridge_family(0.25) == 0.25
    with pytest.raises(ValueError):
        validate_bridge_weight_for_bridge_family(-0.01)
    with pytest.raises(ValueError):
        validate_bridge_weight_for_bridge_family(0.26)


def test_build_ranking_config_bridge_reason_mode_and_persisted_weight() -> None:
    eff0 = resolved_family_weights(0.0)
    effw = resolved_family_weights(0.12)
    c0 = _build_ranking_config(
        corpus_snapshot_version="snap",
        placeholder_policy="p",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
        cluster_version="k1",
        embedding_version="ev",
        bridge_weight_for_bridge_family=0.0,
        family_weights_resolved=eff0,
    )
    cw = _build_ranking_config(
        corpus_snapshot_version="snap",
        placeholder_policy="p",
        low_cite_min_year=2019,
        low_cite_max_citations=30,
        cluster_version="k1",
        embedding_version="ev",
        bridge_weight_for_bridge_family=0.12,
        family_weights_resolved=effw,
    )
    ca0 = c0["clustering_artifact"]
    caw = cw["clustering_artifact"]
    assert isinstance(ca0, dict) and isinstance(caw, dict)
    assert ca0["bridge_weight_in_final_score"] == 0.0
    assert ca0["bridge_reason_mode"] == "structural_zero_weight"
    assert ca0["neighbor_mix_v1"] == {
        "signal_version": "neighbor_mix_v1",
        "k": NEIGHBOR_MIX_V1_DEFAULT_K,
    }
    assert caw["bridge_weight_in_final_score"] == 0.12
    assert caw["bridge_reason_mode"] == "structural_weighted"
    assert caw["neighbor_mix_v1"] == {
        "signal_version": "neighbor_mix_v1",
        "k": NEIGHBOR_MIX_V1_DEFAULT_K,
    }
    assert c0["family_weights"]["bridge"]["bridge"] == 0.0
    assert cw["family_weights"]["bridge"]["bridge"] == 0.12


def test_bridge_family_persists_score_when_cluster_context_but_final_score_unchanged() -> None:
    candidates = [
        _pool_candidate(work_id=10, topic_ids=(1, 2, 3), citation_count=8),
        _pool_candidate(work_id=20, topic_ids=(1,), citation_count=1),
    ]
    bridge_map = {10: 0.99, 20: 0.101}
    rows_base = build_step3_heuristic_score_rows(candidates)
    rows_sig = build_step3_heuristic_score_rows(
        candidates,
        cluster_version="cluster-v0",
        bridge_boundary_by_work=bridge_map,
    )
    for wid in (10, 20):
        base_b = next(r for r in rows_base if r.work_id == wid and r.recommendation_family == "bridge")
        sig_b = next(r for r in rows_sig if r.work_id == wid and r.recommendation_family == "bridge")
        assert base_b.final_score == sig_b.final_score
        assert sig_b.bridge_score is not None
        assert sig_b.semantic_score is None
        assert sig_b.reason_short == BRIDGE_REASON_STRUCTURAL


def test_bridge_legacy_reason_when_cluster_version_not_pinned() -> None:
    rows = build_step3_heuristic_score_rows([_pool_candidate(work_id=42)])
    b = next(r for r in rows if r.recommendation_family == "bridge")
    assert b.bridge_score is None
    assert b.reason_short == BRIDGE_REASON_LEGACY


def test_bridge_no_cluster_reason_when_score_missing_in_map() -> None:
    c = _pool_candidate(work_id=7, topic_ids=(1, 2))
    rows = build_step3_heuristic_score_rows(
        [c],
        cluster_version="cv",
        bridge_boundary_by_work={},
    )
    b = next(r for r in rows if r.recommendation_family == "bridge")
    assert b.bridge_score is None
    assert b.reason_short == BRIDGE_REASON_NO_CLUSTER


def test_bridge_map_value_none_uses_no_cluster_not_weighted_copy() -> None:
    """Explicit None in the boundary map must not pair with structural weighted reason_short."""
    c = _pool_candidate(work_id=7, topic_ids=(1, 2))
    rows = build_step3_heuristic_score_rows(
        [c],
        cluster_version="cv",
        bridge_boundary_by_work={7: None},
        bridge_weight_for_bridge_family=0.06,
    )
    b = next(r for r in rows if r.recommendation_family == "bridge")
    assert b.bridge_score is None
    assert b.reason_short == BRIDGE_REASON_NO_CLUSTER
    assert b.reason_short != BRIDGE_REASON_STRUCTURAL_WEIGHTED


def test_structural_bridge_reason_always_has_bridge_score() -> None:
    """Invariant: centroid-boundary copy is only emitted when a numeric bridge signal exists."""
    candidates = [
        _pool_candidate(work_id=1, topic_ids=(1, 2), citation_count=3),
        _pool_candidate(work_id=2, topic_ids=(1,), citation_count=1),
    ]
    for bw in (0.0, 0.06, 0.12):
        rows = build_step3_heuristic_score_rows(
            candidates,
            cluster_version="cv",
            bridge_boundary_by_work={1: 0.7, 2: 0.2},
            bridge_weight_for_bridge_family=bw,
        )
        for r in rows:
            if r.recommendation_family != "bridge":
                continue
            if r.reason_short in (
                BRIDGE_REASON_STRUCTURAL_ZERO_WEIGHT,
                BRIDGE_REASON_STRUCTURAL_WEIGHTED,
            ):
                assert r.bridge_score is not None, (bw, r.work_id, r.reason_short)


def test_ranking_counts_from_rows() -> None:
    rows = build_step3_heuristic_score_rows(
        [_pool_candidate(work_id=1, year=2026, citation_count=0, topic_ids=(1,))]
    )
    c = _ranking_counts_from_rows(1, rows)
    assert c.total_candidate_works == 1
    assert c.total_rows_written == 3
    assert c.rows_null_semantic == 3
    assert c.rows_null_bridge == 3
    assert c.rows_by_family["emerging"] == 1
    assert c.rows_by_family["undercited"] == 1


def test_neighbor_mix_fields_on_bridge_family_when_map_present() -> None:
    mix = NeighborMixV1Result(
        eligible=True,
        mix_score=0.4,
        neighbor_work_ids=(2, 3, 4),
        anchor_cluster_id="c0",
        foreign_neighbor_count=6,
    )
    k = 15
    c = _pool_candidate(work_id=10)
    rows = build_step3_heuristic_score_rows(
        [c],
        cluster_version="cv",
        bridge_boundary_by_work={10: 0.5},
        neighbor_mix_by_work={10: mix},
        neighbor_mix_k=k,
    )
    b = next(r for r in rows if r.recommendation_family == "bridge")
    assert b.bridge_eligible is True
    assert b.bridge_signal_json is not None
    assert b.bridge_signal_json["signal_version"] == "neighbor_mix_v1"
    assert b.bridge_signal_json["k"] == k
    assert b.bridge_signal_json["mix_score"] == 0.4


def test_neighbor_mix_null_on_non_bridge_families_when_map_present() -> None:
    mix = NeighborMixV1Result(
        eligible=True,
        mix_score=0.4,
        neighbor_work_ids=(2, 3, 4),
        anchor_cluster_id="c0",
        foreign_neighbor_count=6,
    )
    c = _pool_candidate(work_id=10)
    rows = build_step3_heuristic_score_rows(
        [c],
        cluster_version="cv",
        bridge_boundary_by_work={10: 0.5},
        neighbor_mix_by_work={10: mix},
    )
    for r in rows:
        if r.recommendation_family == "bridge":
            continue
        assert r.bridge_eligible is None
        assert r.bridge_signal_json is None


def test_neighbor_mix_absent_when_work_not_in_map() -> None:
    rows = build_step3_heuristic_score_rows(
        [_pool_candidate(work_id=10)],
        cluster_version="cv",
        bridge_boundary_by_work={10: 0.5},
        neighbor_mix_by_work={},
    )
    for r in rows:
        assert r.bridge_eligible is None
        assert r.bridge_signal_json is None
