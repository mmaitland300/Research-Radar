from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from app.public_release_repo import (
    MaterializedRunContext,
    PublicReleasePromotion,
    fetch_latest_public_release_promotion,
    fetch_succeeded_materialized_run,
    inspect_public_release_serveability,
)


class _Result:
    def __init__(self, *, one: Any = None, all_rows: list[Any] | None = None) -> None:
        self._one = one
        self._all = list(all_rows or [])

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _Conn:
    def __init__(self, results: list[_Result]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _Result:
        self.calls.append((sql, tuple(params)))
        return self.results.pop(0)


def _run(*, config: dict[str, Any] | None = None, status: str = "succeeded") -> MaterializedRunContext:
    return MaterializedRunContext(
        ranking_run_id="rank-active",
        ranking_version="ranking-v1",
        corpus_snapshot_version="snapshot-composed",
        embedding_version="embedding-v1",
        status=status,
        finished_at=datetime(2026, 8, 23, tzinfo=UTC),
        error_message=None,
        config_json=(
            {
                "families_written": ["emerging", "bridge", "undercited"],
                "clustering_artifact": None,
                "selection_scope": {
                    "type": "included_works",
                    "corpus_snapshot_version": "snapshot-composed",
                },
            }
            if config is None
            else config
        ),
        counts_json={
            "total_candidate_works": 2,
            "total_rows_written": 5,
            "rows_by_family": {"emerging": 2, "bridge": 2, "undercited": 1},
        },
    )


def _promotion(*, run: MaterializedRunContext | None = None) -> PublicReleasePromotion:
    return PublicReleasePromotion(
        promotion_id=7,
        promoted_at=datetime(2026, 8, 23, tzinfo=UTC),
        promoted_by="test",
        note=None,
        run=run or _run(),
    )


def _complete_score_rows(*, outside: int = 0) -> list[dict[str, Any]]:
    return [
        {
            "recommendation_family": "emerging",
            "member_score_count": 2,
            "out_of_membership_count": outside,
        },
        {
            "recommendation_family": "bridge",
            "member_score_count": 2,
            "out_of_membership_count": 0,
        },
        {
            "recommendation_family": "undercited",
            "member_score_count": 1,
            "out_of_membership_count": 0,
        },
    ]


def _clustered_config() -> dict[str, Any]:
    return {
        "families_written": ["emerging", "bridge", "undercited"],
        "selection_scope": {
            "type": "included_works",
            "corpus_snapshot_version": "snapshot-composed",
        },
        "clustering_artifact": {
            "cluster_version": "cluster-v1",
            "corpus_snapshot_version": "snapshot-composed",
            "embedding_version": "embedding-v1",
        },
    }


def _complete_cluster_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "status": "succeeded",
        "finished_at": datetime(2026, 8, 23, tzinfo=UTC),
        "error_message": None,
        "corpus_snapshot_version": "snapshot-composed",
        "embedding_version": "embedding-v1",
        "member_assignment_count": 2,
        "out_of_membership_count": 0,
    }
    row.update(overrides)
    return row


def test_fetch_latest_promotion_includes_referenced_run() -> None:
    row = {
        "promotion_id": 7,
        "promoted_at": datetime(2026, 8, 23, tzinfo=UTC),
        "promoted_by": "operator",
        "note": "known-good",
        "ranking_run_id": "rank-active",
        "ranking_version": "ranking-v1",
        "corpus_snapshot_version": "snapshot-composed",
        "embedding_version": "embedding-v1",
        "status": "succeeded",
        "finished_at": datetime(2026, 8, 23, tzinfo=UTC),
        "error_message": None,
        "config_json": {"families_written": ["emerging", "bridge", "undercited"]},
        "counts_json": {"rows_by_family": {"emerging": 2}},
    }
    conn = _Conn([_Result(one=row)])

    promotion = fetch_latest_public_release_promotion(conn)  # type: ignore[arg-type]

    assert promotion is not None
    assert promotion.promotion_id == 7
    assert promotion.run.ranking_run_id == "rank-active"
    assert promotion.scorer_kind == "materialized_paper_scores"
    assert "ORDER BY pr.promotion_id DESC" in conn.calls[0][0]


def test_fetch_exact_run_requires_succeeded_status() -> None:
    row = {
        "ranking_run_id": "rank-explicit",
        "ranking_version": "ranking-v2",
        "corpus_snapshot_version": "snapshot-2",
        "embedding_version": "embedding-2",
        "status": "succeeded",
        "finished_at": datetime(2026, 8, 23, tzinfo=UTC),
        "error_message": None,
        "config_json": "{}",
        "counts_json": "{}",
    }
    conn = _Conn([_Result(one=row)])

    run = fetch_succeeded_materialized_run(conn, ranking_run_id="rank-explicit")  # type: ignore[arg-type]

    assert run is not None
    assert run.ranking_run_id == "rank-explicit"
    assert conn.calls[0][1] == ("rank-explicit",)
    assert "status = 'succeeded'" in conn.calls[0][0]


def test_serveability_accepts_complete_materialized_release() -> None:
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 2}),
            _Result(all_rows=_complete_score_rows()),
        ]
    )

    diagnostics = inspect_public_release_serveability(conn, _promotion())  # type: ignore[arg-type]

    assert diagnostics.serveable is True
    assert diagnostics.membership_count == 2
    assert diagnostics.family_score_counts == {
        "emerging": 2,
        "bridge": 2,
        "undercited": 1,
    }
    assert diagnostics.failures == ()


def test_serveability_rejects_placeholder_embedding_version() -> None:
    run = replace(_run(), embedding_version="none-v0")
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 2}),
            _Result(all_rows=_complete_score_rows()),
        ]
    )

    diagnostics = inspect_public_release_serveability(  # type: ignore[arg-type]
        conn, _promotion(run=run)
    )

    assert diagnostics.serveable is False
    assert "embedding_version_not_serveable" in diagnostics.failures


def test_serveability_reports_embedding_family_and_membership_gaps() -> None:
    score_rows = _complete_score_rows(outside=1)
    score_rows[0]["member_score_count"] = 1
    score_rows[1]["member_score_count"] = 0
    score_rows[2]["member_score_count"] = 0
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 1}),
            _Result(all_rows=score_rows),
        ]
    )

    diagnostics = inspect_public_release_serveability(conn, _promotion())  # type: ignore[arg-type]

    assert diagnostics.serveable is False
    assert diagnostics.missing_embedding_count == 1
    assert diagnostics.out_of_membership_score_count == 1
    assert "embedding_coverage_incomplete" in diagnostics.failures
    assert "family_score_count_mismatch:emerging" in diagnostics.failures
    assert "family_score_rows_missing:bridge" in diagnostics.failures
    assert "ranking_rows_outside_snapshot_membership" in diagnostics.failures


def test_serveability_rechecks_promoted_run_completion_and_error_state() -> None:
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 2}),
            _Result(all_rows=_complete_score_rows()),
        ]
    )
    run = replace(_run(), finished_at=None, error_message="late mutation")

    diagnostics = inspect_public_release_serveability(  # type: ignore[arg-type]
        conn, _promotion(run=run)
    )

    assert diagnostics.serveable is False
    assert "ranking_run_not_finished" in diagnostics.failures
    assert "ranking_run_has_error" in diagnostics.failures


def test_serveability_rejects_noninteger_persisted_counts() -> None:
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 2}),
            _Result(all_rows=_complete_score_rows()),
        ]
    )
    counts = dict(_run().counts_json)
    counts["rows_by_family"] = {
        "emerging": 2.9,
        "bridge": 2,
        "undercited": 1,
    }

    diagnostics = inspect_public_release_serveability(  # type: ignore[arg-type]
        conn, _promotion(run=replace(_run(), counts_json=counts))
    )

    assert diagnostics.serveable is False
    assert "expected_family_score_counts_missing" in diagnostics.failures


def test_serveability_requires_snapshot_wide_emerging_and_bridge_coverage() -> None:
    run = replace(
        _run(),
        counts_json={
            "total_candidate_works": 2,
            "total_rows_written": 3,
            "rows_by_family": {"emerging": 1, "bridge": 1, "undercited": 1},
        },
    )
    score_rows = _complete_score_rows()
    score_rows[0]["member_score_count"] = 1
    score_rows[1]["member_score_count"] = 1
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 2}),
            _Result(all_rows=score_rows),
        ]
    )

    diagnostics = inspect_public_release_serveability(  # type: ignore[arg-type]
        conn, _promotion(run=run)
    )

    assert diagnostics.serveable is False
    assert "family_snapshot_coverage_incomplete:emerging" in diagnostics.failures
    assert "family_snapshot_coverage_incomplete:bridge" in diagnostics.failures


def test_serveability_requires_materialized_selection_scope() -> None:
    run = _run(config={"families_written": ["emerging", "bridge", "undercited"]})
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 2}),
            _Result(all_rows=_complete_score_rows()),
        ]
    )

    diagnostics = inspect_public_release_serveability(  # type: ignore[arg-type]
        conn, _promotion(run=run)
    )

    assert diagnostics.serveable is False
    assert "materialized_selection_scope_invalid" in diagnostics.failures


def test_serveability_checks_clustering_when_run_config_identifies_it() -> None:
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 2}),
            _Result(all_rows=_complete_score_rows()),
            _Result(one=_complete_cluster_row()),
        ]
    )

    diagnostics = inspect_public_release_serveability(  # type: ignore[arg-type]
        conn, _promotion(run=_run(config=_clustered_config()))
    )

    assert diagnostics.serveable is True
    assert diagnostics.cluster_version == "cluster-v1"
    assert diagnostics.cluster_assignment_count == 2
    assert diagnostics.missing_cluster_assignment_count == 0


@pytest.mark.parametrize(
    ("cluster_overrides", "expected_failure"),
    [
        ({"finished_at": None}, "clustering_run_not_finished"),
        ({"error_message": "failed after finalize"}, "clustering_run_has_error"),
    ],
)
def test_serveability_rejects_nonterminal_clustering_run(
    cluster_overrides: dict[str, Any],
    expected_failure: str,
) -> None:
    conn = _Conn(
        [
            _Result(one={"membership_count": 2, "embedding_count": 2}),
            _Result(all_rows=_complete_score_rows()),
            _Result(one=_complete_cluster_row(**cluster_overrides)),
        ]
    )

    diagnostics = inspect_public_release_serveability(  # type: ignore[arg-type]
        conn,
        _promotion(run=_run(config=_clustered_config())),
    )

    assert diagnostics.serveable is False
    assert expected_failure in diagnostics.failures
