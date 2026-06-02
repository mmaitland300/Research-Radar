"""Tests for bridge top-ranked validation worksheet."""

from __future__ import annotations

import pytest

from pipeline.ml_bridge_negative_mining_worksheet import BridgeMiningCandidate
from pipeline.ml_bridge_top_ranked_validation_worksheet import (
    ALLOWED_SAMPLE_REASONS,
    DEFAULT_CONTRASTIVE_N,
    DEFAULT_CONTRASTIVE_RANK_MAX,
    DEFAULT_TOP_N,
    WRITE_SQL_RE,
    MLBridgeTopRankedValidationWorksheetError,
    _execute_select,
    render_csv,
    select_top_ranked_sample,
    stable_row_id,
)


def _cand(
    rank: int,
    *,
    paper_id: str,
    final_score: float,
    bridge_score: float | None = None,
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
        topics_raw='["Music Processing"]',
        abstract="Audio retrieval paper.",
        final_score=final_score,
        semantic_score=None,
        citation_velocity_score=0.1,
        topic_growth_score=1.0,
        bridge_score=bridge_score,
        diversity_penalty=0.0,
        bridge_eligible=None,
        reason_short="test",
    )


def _make_candidates(n: int = 50) -> list[BridgeMiningCandidate]:
    return [
        _cand(i, paper_id=f"https://openalex.org/W{1000 + i}", final_score=1.0 - i / n)
        for i in range(1, n + 1)
    ]


class TestStableRowId:
    def test_deterministic(self) -> None:
        a = stable_row_id(paper_id="https://openalex.org/W123")
        b = stable_row_id(paper_id="https://openalex.org/W123")
        assert a == b
        assert len(a) == 64

    def test_differs_by_paper_id(self) -> None:
        a = stable_row_id(paper_id="https://openalex.org/W123")
        b = stable_row_id(paper_id="https://openalex.org/W456")
        assert a != b


class TestExecuteSelect:
    """_execute_select enforces SELECT-only SQL."""

    class _FakeCur:
        def __init__(self) -> None:
            self.last_sql: str | None = None

        def execute(self, sql: str, params: tuple) -> "_FakeCur":
            self.last_sql = sql
            return self

    def test_allows_select(self) -> None:
        cur = self._FakeCur()
        _execute_select(cur, "SELECT 1")
        assert cur.last_sql == "SELECT 1"

    @pytest.mark.parametrize(
        "bad_sql",
        [
            "INSERT INTO foo VALUES (1)",
            "UPDATE foo SET x=1",
            "DELETE FROM foo",
            "DROP TABLE foo",
            "ALTER TABLE foo ADD COLUMN x INT",
            "TRUNCATE foo",
        ],
    )
    def test_rejects_write_sql(self, bad_sql: str) -> None:
        cur = self._FakeCur()
        with pytest.raises(MLBridgeTopRankedValidationWorksheetError, match="DB safety violation"):
            _execute_select(cur, bad_sql)

    def test_rejects_non_select_prefix(self) -> None:
        cur = self._FakeCur()
        with pytest.raises(MLBridgeTopRankedValidationWorksheetError, match="must start with SELECT"):
            _execute_select(cur, "EXPLAIN SELECT 1")


class TestSelectTopRankedSample:
    def test_takes_top_n_rows(self) -> None:
        candidates = _make_candidates(50)
        selected, debug = select_top_ranked_sample(
            candidates,
            already_labeled_this_run_ids=set(),
            top_n=20,
            contrastive_n=10,
            contrastive_rank_max=40,
        )
        top = [s for s in selected if s.sample_reason == "bridge_top_ranked"]
        assert len(top) == 20
        assert all(s.candidate.family_rank <= 20 for s in top)

    def test_contrastive_excludes_already_labeled(self) -> None:
        candidates = _make_candidates(50)
        # work_tokens for ranks 21-25 (paper_ids are https://openalex.org/W1021 .. W1025)
        already = {f"W{1020 + i}" for i in range(1, 6)}
        selected, debug = select_top_ranked_sample(
            candidates,
            already_labeled_this_run_ids=already,
            top_n=20,
            contrastive_n=10,
            contrastive_rank_max=40,
        )
        contrastive = [s for s in selected if s.sample_reason == "bridge_borderline_contrastive"]
        contrastive_tokens = {s.candidate.work_token for s in contrastive}
        assert contrastive_tokens.isdisjoint(already)

    def test_contrastive_in_rank_window(self) -> None:
        candidates = _make_candidates(50)
        selected, _ = select_top_ranked_sample(
            candidates,
            already_labeled_this_run_ids=set(),
            top_n=20,
            contrastive_n=10,
            contrastive_rank_max=40,
        )
        contrastive = [s for s in selected if s.sample_reason == "bridge_borderline_contrastive"]
        assert all(21 <= s.candidate.family_rank <= 40 for s in contrastive)

    def test_rows_in_rank_ascending_order(self) -> None:
        candidates = _make_candidates(50)
        selected, _ = select_top_ranked_sample(
            candidates,
            already_labeled_this_run_ids=set(),
        )
        ranks = [s.candidate.family_rank for s in selected]
        assert ranks == sorted(ranks)

    def test_contrastive_capped_at_n(self) -> None:
        candidates = _make_candidates(50)
        selected, debug = select_top_ranked_sample(
            candidates,
            already_labeled_this_run_ids=set(),
            top_n=20,
            contrastive_n=5,
            contrastive_rank_max=40,
        )
        contrastive = [s for s in selected if s.sample_reason == "bridge_borderline_contrastive"]
        assert len(contrastive) <= 5

    def test_debug_fields(self) -> None:
        candidates = _make_candidates(50)
        _, debug = select_top_ranked_sample(candidates, already_labeled_this_run_ids=set())
        assert "top_n_achieved" in debug
        assert "contrastive_n_achieved" in debug
        assert "total_rows" in debug
        assert debug["total_rows"] == debug["top_n_achieved"] + debug["contrastive_n_achieved"]


class TestAllowedSampleReasons:
    def test_all_reasons_known(self) -> None:
        assert "bridge_top_ranked" in ALLOWED_SAMPLE_REASONS
        assert "bridge_borderline_contrastive" in ALLOWED_SAMPLE_REASONS


class TestRenderCsv:
    def test_csv_has_header_and_rows(self) -> None:
        candidates = _make_candidates(5)
        from pipeline.ml_bridge_top_ranked_validation_worksheet import (
            TopRankedSelection,
            _candidate_csv_row,
        )

        selections = [
            TopRankedSelection(c, "bridge_top_ranked") for c in candidates
        ]
        rows = [_candidate_csv_row(selection=s) for s in selections]
        text = render_csv(rows)
        lines = text.strip().splitlines()
        assert len(lines) == 6
        assert "row_id" in lines[0]
        assert "relevance_label" in lines[0]

    def test_label_columns_blank(self) -> None:
        candidates = _make_candidates(3)
        from pipeline.ml_bridge_top_ranked_validation_worksheet import (
            TopRankedSelection,
            _candidate_csv_row,
        )
        import csv as _csv, io as _io

        selections = [TopRankedSelection(c, "bridge_top_ranked") for c in candidates]
        rows = [_candidate_csv_row(selection=s) for s in selections]
        text = render_csv(rows)
        reader = _csv.DictReader(_io.StringIO(text))
        for row in reader:
            assert row["relevance_label"] == ""
            assert row["novelty_label"] == ""
            assert row["bridge_like_label"] == ""
