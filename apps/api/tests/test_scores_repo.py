from unittest.mock import MagicMock

from app.scores_repo import (
    _latest_successful_ranking_run_id,
    latest_corpus_snapshot_version_with_works,
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
