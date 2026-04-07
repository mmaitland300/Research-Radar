from unittest.mock import MagicMock, patch

from app.scores_repo import (
    _latest_successful_ranking_run_id,
    latest_corpus_snapshot_version_with_works,
    list_ranked_recommendations,
    resolve_ranked_run_context,
)


def test_latest_corpus_snapshot_version_with_works_uses_dict_row_shape() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = {
        "source_snapshot_version": "source-snapshot-1"
    }

    assert latest_corpus_snapshot_version_with_works(conn) == "source-snapshot-1"


def test_latest_successful_ranking_run_id_uses_dict_row_shape() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = {"ranking_run_id": "rank-123"}

    assert (
        _latest_successful_ranking_run_id(
            conn,
            corpus_snapshot_version="source-snapshot-1",
            ranking_version=None,
        )
        == "rank-123"
    )


def test_resolve_ranked_run_context_explicit_id_uses_dict_row_shape() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = {
        "ranking_run_id": "rank-123",
        "ranking_version": "v0-test",
        "corpus_snapshot_version": "source-snapshot-1",
    }

    ctx = resolve_ranked_run_context(
        conn,
        ranking_run_id="rank-123",
        corpus_snapshot_version=None,
        ranking_version=None,
    )

    assert ctx is not None
    assert ctx.ranking_run_id == "rank-123"
    assert ctx.ranking_version == "v0-test"
    assert ctx.corpus_snapshot_version == "source-snapshot-1"


def test_resolve_ranked_run_context_latest_run_uses_dict_row_shape() -> None:
    conn = MagicMock()
    conn.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value={"ranking_run_id": "rank-456"})),
        MagicMock(fetchone=MagicMock(return_value={
            "ranking_run_id": "rank-456",
            "ranking_version": "v0-test",
            "corpus_snapshot_version": "source-snapshot-2",
        })),
    ]

    ctx = resolve_ranked_run_context(
        conn,
        ranking_run_id=None,
        corpus_snapshot_version="source-snapshot-2",
        ranking_version=None,
    )

    assert ctx is not None
    assert ctx.ranking_run_id == "rank-456"
    assert ctx.ranking_version == "v0-test"
    assert ctx.corpus_snapshot_version == "source-snapshot-2"


@patch("app.scores_repo.database_url_from_env", return_value="postgresql://test")
@patch("app.scores_repo.psycopg.connect")
def test_list_ranked_recommendations_bridge_eligible_filter_sql(
    mock_connect: MagicMock,
    _mock_dsn: MagicMock,
) -> None:
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn

    r_ctx = MagicMock()
    r_ctx.fetchone.return_value = {
        "ranking_run_id": "rank-x",
        "ranking_version": "v0",
        "corpus_snapshot_version": "snap-a",
    }
    r_rows = MagicMock()
    r_rows.fetchall.return_value = []
    r_cfg = MagicMock()
    r_cfg.fetchone.return_value = {"config_json": {}}
    mock_conn.execute.side_effect = [r_ctx, r_rows, r_cfg]

    out = list_ranked_recommendations(
        family="bridge",
        limit=10,
        ranking_run_id="rank-x",
        bridge_eligible_only=True,
    )

    assert out is not None
    list_query = mock_conn.execute.call_args_list[1][0][0]
    assert "ps.bridge_eligible IS TRUE" in list_query


@patch("app.scores_repo.database_url_from_env", return_value="postgresql://test")
@patch("app.scores_repo.psycopg.connect")
def test_list_ranked_recommendations_no_eligible_filter_for_non_bridge_family(
    mock_connect: MagicMock,
    _mock_dsn: MagicMock,
) -> None:
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn

    r_ctx = MagicMock()
    r_ctx.fetchone.return_value = {
        "ranking_run_id": "rank-x",
        "ranking_version": "v0",
        "corpus_snapshot_version": "snap-a",
    }
    r_rows = MagicMock()
    r_rows.fetchall.return_value = []
    r_cfg = MagicMock()
    r_cfg.fetchone.return_value = {"config_json": {}}
    mock_conn.execute.side_effect = [r_ctx, r_rows, r_cfg]

    list_ranked_recommendations(
        family="emerging",
        limit=10,
        ranking_run_id="rank-x",
        bridge_eligible_only=True,
    )

    list_query = mock_conn.execute.call_args_list[1][0][0]
    assert "ps.bridge_eligible IS TRUE" not in list_query
