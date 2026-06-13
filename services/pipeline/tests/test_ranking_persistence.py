from unittest.mock import MagicMock, patch

from pipeline.ranking_persistence import (
    latest_corpus_snapshot_version_with_works,
    latest_successful_ranking_run_id,
)


def test_latest_corpus_snapshot_version_with_works_uses_memberships() -> None:
    conn = MagicMock()
    with patch(
        "pipeline.ranking_persistence.latest_snapshot_with_included_memberships",
        return_value="composed-snapshot",
    ) as latest:
        assert latest_corpus_snapshot_version_with_works(conn) == "composed-snapshot"
    latest.assert_called_once_with(conn)
    conn.execute.assert_not_called()


def test_latest_successful_ranking_run_id_without_version_filter() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = ("rank-abc123",)
    rid = latest_successful_ranking_run_id(conn, corpus_snapshot_version="source-snapshot-x")
    assert rid == "rank-abc123"
    conn.execute.assert_called_once()
    params = conn.execute.call_args[0][1]
    assert params == ("source-snapshot-x",)


def test_latest_successful_ranking_run_id_with_ranking_version() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = ("rank-xyz",)
    rid = latest_successful_ranking_run_id(
        conn, corpus_snapshot_version="snap", ranking_version="v0"
    )
    assert rid == "rank-xyz"
    sql = conn.execute.call_args[0][0]
    assert "ranking_version" in sql


def test_latest_successful_ranking_run_id_none() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None
    assert latest_successful_ranking_run_id(conn, corpus_snapshot_version="snap") is None
