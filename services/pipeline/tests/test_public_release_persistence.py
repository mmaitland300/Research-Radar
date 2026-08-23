from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from pipeline.public_release_persistence import (
    PUBLIC_RELEASE_ADVISORY_LOCK_ID,
    acquire_public_release_advisory_lock,
    append_public_release_promotion,
    fetch_active_public_release_promotion,
    fetch_ranking_run_for_promotion,
    fetch_score_coverage,
)


NOW = datetime(2026, 8, 23, tzinfo=UTC)


def _connection_with_row(row):
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = row
    return conn


def test_fetch_ranking_run_uses_exact_id_and_share_lock() -> None:
    row = {
        "ranking_run_id": "run-1",
        "ranking_version": "rank-v1",
        "corpus_snapshot_version": "snapshot-v1",
        "embedding_version": "embedding-v1",
        "status": "succeeded",
        "finished_at": NOW,
        "config_json": {"kind": "heuristic"},
        "counts_json": {"total_rows_written": 3},
        "error_message": None,
    }
    conn = _connection_with_row(row)

    run = fetch_ranking_run_for_promotion(conn, ranking_run_id="run-1")

    assert run is not None
    assert run.ranking_run_id == "run-1"
    sql, params = conn.execute.call_args.args
    assert "WHERE ranking_run_id = %s" in sql
    assert "FOR SHARE" in sql
    assert params == ("run-1",)


def test_persistence_adapters_accept_default_tuple_rows() -> None:
    conn = _connection_with_row(
        (
            "run-tuple",
            "rank-v1",
            "snapshot-v1",
            "embedding-v1",
            "succeeded",
            NOW,
            {"families_written": []},
            {"total_rows_written": 0},
            None,
        )
    )

    run = fetch_ranking_run_for_promotion(conn, ranking_run_id="run-tuple")

    assert run is not None
    assert run.ranking_run_id == "run-tuple"
    assert run.finished_at == NOW


def test_advisory_lock_uses_stable_transaction_key() -> None:
    conn = MagicMock()

    acquire_public_release_advisory_lock(conn)

    conn.execute.assert_called_once_with(
        "SELECT pg_advisory_xact_lock(%s)", (PUBLIC_RELEASE_ADVISORY_LOCK_ID,)
    )


def test_fetch_active_release_uses_append_order() -> None:
    conn = _connection_with_row(
        {
            "promotion_id": 9,
            "ranking_run_id": "run-9",
            "promoted_at": NOW,
            "promoted_by": "pipeline-cli",
            "note": None,
        }
    )

    active = fetch_active_public_release_promotion(conn)

    assert active is not None
    assert active.promotion_id == 9
    sql = conn.execute.call_args.args[0]
    assert "ORDER BY promotion_id DESC" in sql
    assert "LIMIT 1" in sql


def test_score_coverage_is_membership_scoped() -> None:
    conn = _connection_with_row(
        {
            "emerging_count": 8,
            "bridge_count": 8,
            "undercited_count": 3,
            "total_count": 19,
            "outside_membership_count": 0,
        }
    )

    coverage = fetch_score_coverage(
        conn,
        ranking_run_id="run-1",
        corpus_snapshot_version="snapshot-v1",
    )

    assert coverage.rows_by_family == {"emerging": 8, "bridge": 8, "undercited": 3}
    sql, params = conn.execute.call_args.args
    assert "work_source_snapshot_memberships" in sql
    assert "wssm.inclusion_status = 'included'" in sql
    assert params == ("snapshot-v1", "run-1")


def test_append_records_cli_actor_and_returns_database_identity() -> None:
    conn = _connection_with_row(
        {
            "promotion_id": 10,
            "ranking_run_id": "run-1",
            "promoted_at": NOW,
            "promoted_by": "pipeline-cli",
            "note": None,
        }
    )

    promotion = append_public_release_promotion(
        conn,
        ranking_run_id="run-1",
        promoted_by="pipeline-cli",
        note=None,
    )

    assert promotion.promotion_id == 10
    sql, params = conn.execute.call_args.args
    assert "INSERT INTO public_release_promotions" in sql
    assert "RETURNING promotion_id" in sql
    assert params == ("run-1", "pipeline-cli", None)
