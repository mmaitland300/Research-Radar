from pipeline.config import RankingCounts, RankingRun
from pipeline.ranking_run import (
    RECOMMENDATION_FAMILIES,
    _ranking_counts_from_rows,
    build_step2_stub_score_rows,
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


def test_build_step2_stub_score_rows_shape() -> None:
    rows = build_step2_stub_score_rows([10, 20])
    assert len(rows) == 6
    assert {r.work_id for r in rows} == {10, 20}
    for r in rows:
        assert r.recommendation_family in RECOMMENDATION_FAMILIES
        assert r.semantic_score is None
        assert r.bridge_score is None
        assert r.citation_velocity_score == 0.0
        assert r.reason_short
        assert isinstance(r.final_score, float)


def test_ranking_counts_from_rows() -> None:
    rows = build_step2_stub_score_rows([1])
    c = _ranking_counts_from_rows(1, rows)
    assert c.total_candidate_works == 1
    assert c.total_rows_written == 3
    assert c.rows_null_semantic == 3
    assert c.rows_null_bridge == 3
    assert c.rows_by_family["emerging"] == 1
