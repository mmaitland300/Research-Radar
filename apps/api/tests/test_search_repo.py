from unittest.mock import MagicMock

import app.search_repo as search_repo
from app.scores_repo import RankedRunContext
from app.search_repo import search_papers


def _connection_with_no_search_rows() -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []
    return conn


def test_family_filtered_search_uses_resolved_snapshot_membership(monkeypatch) -> None:
    conn = _connection_with_no_search_rows()
    monkeypatch.setattr(search_repo.psycopg, "connect", lambda *a, **k: conn)
    monkeypatch.setattr(search_repo, "database_url_from_env", lambda: "postgresql://test")
    monkeypatch.setattr(
        search_repo,
        "resolve_ranked_run_context",
        lambda *_a, **_k: RankedRunContext(
            ranking_run_id="rank-1",
            ranking_version="rank-v1",
            corpus_snapshot_version="snapshot-composed",
        ),
    )

    result = search_papers(
        q="music retrieval",
        limit=10,
        offset=0,
        year_from=2020,
        year_to=2025,
        included_scope="core",
        source_slug="ismir",
        topic="audio",
        family_hint="emerging",
    )

    assert result.resolved_corpus_snapshot_version == "snapshot-composed"
    query, params = conn.execute.call_args[0]
    assert "work_source_snapshot_memberships wssm" in query
    assert "wssm.work_id = w.id" in query
    assert "wssm.source_snapshot_version = %s" in query
    assert "wssm.inclusion_status = 'included'" in query
    assert params == [
        "music retrieval",
        "snapshot-composed",
        2020,
        2025,
        "ismir",
        "%audio%",
        "rank-1",
        "emerging",
        10,
        0,
    ]


def test_lexical_only_search_does_not_add_snapshot_membership(monkeypatch) -> None:
    conn = _connection_with_no_search_rows()
    monkeypatch.setattr(search_repo.psycopg, "connect", lambda *a, **k: conn)
    monkeypatch.setattr(search_repo, "database_url_from_env", lambda: "postgresql://test")
    monkeypatch.setattr(
        search_repo,
        "resolve_ranked_run_context",
        MagicMock(side_effect=AssertionError("lexical search must not resolve a ranking run")),
    )

    result = search_papers(q="music retrieval", limit=10, offset=0)

    assert result.resolved_corpus_snapshot_version is None
    query, params = conn.execute.call_args[0]
    assert "work_source_snapshot_memberships" not in query
    assert params == ["music retrieval", 10, 0]
