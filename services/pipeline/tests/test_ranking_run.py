from pipeline.config import RankingCounts, RankingRun
from pipeline.ranking import RankingCandidate
from pipeline.ranking_run import (
    RECOMMENDATION_FAMILIES,
    _ranking_counts_from_rows,
    build_step3_heuristic_score_rows,
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
        RankingCandidate(work_id=10, year=2026, citation_count=8, topic_ids=(1, 2, 3)),
        RankingCandidate(work_id=20, year=2026, citation_count=1, topic_ids=(1,)),
        RankingCandidate(work_id=30, year=2024, citation_count=20, topic_ids=(2,)),
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


def test_ranking_counts_from_rows() -> None:
    rows = build_step3_heuristic_score_rows([RankingCandidate(work_id=1, year=2026, citation_count=0, topic_ids=(1,))])
    c = _ranking_counts_from_rows(1, rows)
    assert c.total_candidate_works == 1
    assert c.total_rows_written == 3
    assert c.rows_null_semantic == 3
    assert c.rows_null_bridge == 3
    assert c.rows_by_family["emerging"] == 1