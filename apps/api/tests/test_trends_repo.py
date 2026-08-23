from types import SimpleNamespace
from unittest.mock import MagicMock

import app.trends_repo as trends_repo
from app.trends_repo import list_topic_trends


def test_list_topic_trends_uses_explicit_snapshot(monkeypatch) -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []

    monkeypatch.setattr(trends_repo.psycopg, "connect", lambda *a, **k: conn)
    resolve_context = MagicMock(
        return_value=SimpleNamespace(
            corpus_snapshot_version="source-snapshot-explicit",
        )
    )
    monkeypatch.setattr(trends_repo, "resolve_serving_context", resolve_context)

    result = list_topic_trends(
        limit=5,
        since_year=2025,
        min_works=2,
        corpus_snapshot_version="source-snapshot-explicit",
    )

    assert result.corpus_snapshot_version == "source-snapshot-explicit"
    call = conn.execute.call_args
    query = call[0][0]
    params = call[0][1]
    assert "work_source_snapshot_memberships wssm" in query
    assert "wssm.work_id = w.id" in query
    assert "wssm.source_snapshot_version = %s" in query
    assert "wssm.inclusion_status = 'included'" in query
    assert "w.corpus_snapshot_version = %s" not in query
    assert params[2] == "source-snapshot-explicit"
    resolve_context.assert_called_once_with(
        conn,
        ranking_run_id=None,
        corpus_snapshot_version="source-snapshot-explicit",
        ranking_version=None,
    )


def test_list_topic_trends_defaults_to_active_serving_context(monkeypatch) -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []

    monkeypatch.setattr(trends_repo.psycopg, "connect", lambda *a, **k: conn)
    resolve_context = MagicMock(
        return_value=SimpleNamespace(
            corpus_snapshot_version="source-snapshot-active",
        )
    )
    monkeypatch.setattr(trends_repo, "resolve_serving_context", resolve_context)

    result = list_topic_trends(limit=5, since_year=2025, min_works=2)

    assert result.corpus_snapshot_version == "source-snapshot-active"
    query, params = conn.execute.call_args[0]
    assert "work_source_snapshot_memberships wssm" in query
    assert "wssm.source_snapshot_version = %s" in query
    assert "wssm.inclusion_status = 'included'" in query
    assert params[2] == "source-snapshot-active"
    resolve_context.assert_called_once_with(
        conn,
        ranking_run_id=None,
        corpus_snapshot_version=None,
        ranking_version=None,
    )


def test_list_topic_trends_uses_snapshot_from_exact_run_context(monkeypatch) -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []
    resolve_context = MagicMock(
        return_value=SimpleNamespace(
            corpus_snapshot_version="source-snapshot-from-run",
        )
    )

    monkeypatch.setattr(trends_repo.psycopg, "connect", lambda *a, **k: conn)
    monkeypatch.setattr(trends_repo, "resolve_serving_context", resolve_context)

    result = list_topic_trends(
        limit=5,
        since_year=2025,
        min_works=2,
        corpus_snapshot_version="source-snapshot-conflicting",
        ranking_run_id="rank-historical",
        ranking_version="rank-v-old",
    )

    assert result.corpus_snapshot_version == "source-snapshot-from-run"
    _, params = conn.execute.call_args[0]
    assert params[2] == "source-snapshot-from-run"
    resolve_context.assert_called_once_with(
        conn,
        ranking_run_id="rank-historical",
        corpus_snapshot_version="source-snapshot-conflicting",
        ranking_version="rank-v-old",
    )

