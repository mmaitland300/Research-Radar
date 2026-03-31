from pipeline.config import RankingCounts, RankingRun
from pipeline.ranking import RankingCandidate
from pipeline.ranking_run import (
    BRIDGE_REASON_LEGACY,
    BRIDGE_REASON_NO_CLUSTER,
    BRIDGE_REASON_STRUCTURAL,
    RECOMMENDATION_FAMILIES,
    _ranking_counts_from_rows,
    build_step3_heuristic_score_rows,
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
