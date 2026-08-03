from unittest.mock import MagicMock

import app.trends_repo as trends_repo
from app.trends_repo import list_topic_trends


def test_list_topic_trends_uses_explicit_snapshot(monkeypatch) -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []

    monkeypatch.setattr(trends_repo.psycopg, "connect", lambda *a, **k: conn)
    monkeypatch.setattr(
        trends_repo,
        "latest_corpus_snapshot_version_with_works",
        lambda _conn: "source-snapshot-latest-ignored",
    )

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


def test_list_topic_trends_defaults_to_latest_snapshot(monkeypatch) -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []

    monkeypatch.setattr(trends_repo.psycopg, "connect", lambda *a, **k: conn)
    monkeypatch.setattr(
        trends_repo,
        "latest_corpus_snapshot_version_with_works",
        lambda _conn: "source-snapshot-latest",
    )

    result = list_topic_trends(limit=5, since_year=2025, min_works=2)

    assert result.corpus_snapshot_version == "source-snapshot-latest"
    query, params = conn.execute.call_args[0]
    assert "work_source_snapshot_memberships wssm" in query
    assert "wssm.source_snapshot_version = %s" in query
    assert "wssm.inclusion_status = 'included'" in query
    assert params[2] == "source-snapshot-latest"

