from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from app.public_release_repo import MaterializedRunContext, PublicReleasePromotion
from app.serving_context import (
    ServingContextNotFoundError,
    ServingContextUnavailableError,
    load_serving_context,
    resolve_serving_context,
)


class _Result:
    def __init__(self, row: Any = None) -> None:
        self.row = row

    def fetchone(self) -> Any:
        return self.row


class _Conn:
    def __init__(self, result: _Result | list[_Result] | None = None) -> None:
        self.results = result if isinstance(result, list) else [result]
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _Result:
        self.calls.append((sql, tuple(params)))
        result = self.results.pop(0)
        assert result is not None
        return result


def _run(
    *,
    ranking_run_id: str = "rank-active",
    ranking_version: str = "ranking-v1",
    corpus_snapshot_version: str = "snapshot-composed",
    embedding_version: str = "embedding-v1",
) -> MaterializedRunContext:
    return MaterializedRunContext(
        ranking_run_id=ranking_run_id,
        ranking_version=ranking_version,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        status="succeeded",
        finished_at=datetime(2026, 8, 23, tzinfo=UTC),
        error_message=None,
        config_json={},
        counts_json={},
    )


def _promotion(run: MaterializedRunContext | None = None) -> PublicReleasePromotion:
    return PublicReleasePromotion(
        promotion_id=17,
        promoted_at=datetime(2026, 8, 23, tzinfo=UTC),
        promoted_by="test",
        note=None,
        run=run or _run(),
    )


def _explode(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("unexpected resolution branch")


def test_exact_run_wins_and_reports_its_actual_context(monkeypatch) -> None:
    actual = _run(
        ranking_run_id="rank-exact",
        ranking_version="actual-ranking",
        corpus_snapshot_version="actual-snapshot",
        embedding_version="actual-embedding",
    )
    monkeypatch.setattr(
        "app.serving_context.fetch_succeeded_materialized_run",
        lambda _conn, *, ranking_run_id: actual if ranking_run_id == "rank-exact" else None,
    )
    monkeypatch.setattr(
        "app.serving_context.fetch_latest_public_release_promotion",
        _explode,
    )

    context = resolve_serving_context(
        object(),  # type: ignore[arg-type]
        ranking_run_id="  rank-exact  ",
        corpus_snapshot_version="conflicting-snapshot",
        ranking_version="conflicting-ranking",
    )

    assert context.source == "explicit_run"
    assert context.ranking_run_id == "rank-exact"
    assert context.ranking_version == "actual-ranking"
    assert context.corpus_snapshot_version == "actual-snapshot"
    assert context.embedding_version == "actual-embedding"
    assert context.promotion_id is None
    assert context.is_active_promotion is False


def test_missing_exact_run_raises_not_found(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.serving_context.fetch_succeeded_materialized_run",
        lambda _conn, *, ranking_run_id: None,
    )

    with pytest.raises(ServingContextNotFoundError) as error:
        resolve_serving_context(
            object(),  # type: ignore[arg-type]
            ranking_run_id=" missing ",
            corpus_snapshot_version="ignored",
        )

    assert error.value.ranking_run_id == "missing"
    assert error.value.corpus_snapshot_version is None


def test_snapshot_and_version_constrain_latest_succeeded_run(monkeypatch) -> None:
    conn = _Conn(_Result({"ranking_run_id": "rank-constrained"}))
    constrained = _run(ranking_run_id="rank-constrained")
    monkeypatch.setattr(
        "app.serving_context.fetch_succeeded_materialized_run",
        lambda _conn, *, ranking_run_id: (
            constrained if ranking_run_id == "rank-constrained" else None
        ),
    )

    context = resolve_serving_context(
        conn,  # type: ignore[arg-type]
        ranking_run_id="  ",
        corpus_snapshot_version=" snapshot-composed ",
        ranking_version=" ranking-v1 ",
    )

    assert context.source == "explicit_constraints"
    assert context.ranking_run_id == "rank-constrained"
    sql, params = conn.calls[0]
    assert "corpus_snapshot_version = %s" in sql
    assert "ranking_version = %s" in sql
    assert "status = 'succeeded'" in sql
    assert "ORDER BY finished_at DESC NULLS LAST" in sql
    assert params == ("snapshot-composed", "ranking-v1")


@pytest.mark.parametrize(
    ("snapshot", "version", "expected_clause", "expected_params"),
    [
        ("snapshot-only", None, "corpus_snapshot_version = %s", ("snapshot-only",)),
        (None, "version-only", "ranking_version = %s", ("version-only",)),
    ],
)
def test_each_constraint_is_independently_supported(
    monkeypatch,
    snapshot: str | None,
    version: str | None,
    expected_clause: str,
    expected_params: tuple[str, ...],
) -> None:
    conn = _Conn(_Result({"ranking_run_id": "rank-constrained"}))
    monkeypatch.setattr(
        "app.serving_context.fetch_succeeded_materialized_run",
        lambda _conn, *, ranking_run_id: _run(ranking_run_id=ranking_run_id),
    )

    resolve_serving_context(
        conn,  # type: ignore[arg-type]
        corpus_snapshot_version=snapshot,
        ranking_version=version,
    )

    sql, params = conn.calls[0]
    assert expected_clause in sql
    assert params == expected_params


def test_missing_constrained_run_raises_not_found() -> None:
    conn = _Conn(_Result())

    with pytest.raises(ServingContextNotFoundError) as error:
        resolve_serving_context(
            conn,  # type: ignore[arg-type]
            corpus_snapshot_version="snapshot-missing",
            ranking_version="version-missing",
        )

    assert error.value.corpus_snapshot_version == "snapshot-missing"
    assert error.value.ranking_version == "version-missing"


def test_no_selectors_resolves_valid_active_promotion(monkeypatch) -> None:
    promotion = _promotion()
    monkeypatch.setattr(
        "app.serving_context.fetch_latest_public_release_promotion",
        lambda _conn: promotion,
    )
    monkeypatch.setattr(
        "app.serving_context.inspect_public_release_serveability",
        lambda _conn, _promotion: SimpleNamespace(serveable=True, failures=()),
    )
    monkeypatch.setattr("app.serving_context._legacy_fallback_run", _explode)

    context = resolve_serving_context(
        object(),  # type: ignore[arg-type]
        ranking_run_id=" ",
        corpus_snapshot_version="\t",
        ranking_version="",
    )

    assert context.source == "active_promotion"
    assert context.promotion_id == 17
    assert context.is_active_promotion is True
    assert context.embedding_version == "embedding-v1"


def test_unserveable_active_promotion_fails_closed(monkeypatch) -> None:
    promotion = _promotion()
    monkeypatch.setattr(
        "app.serving_context.fetch_latest_public_release_promotion",
        lambda _conn: promotion,
    )
    monkeypatch.setattr(
        "app.serving_context.inspect_public_release_serveability",
        lambda _conn, _promotion: SimpleNamespace(
            serveable=False,
            failures=("embedding_coverage_incomplete",),
        ),
    )
    monkeypatch.setattr("app.serving_context._legacy_fallback_run", _explode)

    with pytest.raises(ServingContextUnavailableError) as error:
        resolve_serving_context(object())  # type: ignore[arg-type]

    assert error.value.promotion_id == 17
    assert error.value.ranking_run_id == "rank-active"
    assert error.value.failures == ("embedding_coverage_incomplete",)


def test_empty_promotion_table_uses_temporary_legacy_fallback(monkeypatch) -> None:
    run = _run(
        ranking_run_id="rank-legacy",
        ranking_version="legacy-ranking",
        corpus_snapshot_version="legacy-snapshot",
        embedding_version="legacy-embedding",
    )
    monkeypatch.setattr(
        "app.serving_context.fetch_latest_public_release_promotion",
        lambda _conn: None,
    )
    monkeypatch.setattr(
        "app.serving_context.fetch_succeeded_materialized_run",
        lambda _conn, *, ranking_run_id: run if ranking_run_id == "rank-legacy" else None,
    )
    conn = _Conn(
        [
            _Result({"source_snapshot_version": "legacy-snapshot"}),
            _Result({"ranking_run_id": "rank-legacy"}),
        ]
    )

    context = resolve_serving_context(conn)  # type: ignore[arg-type]

    assert context.source == "legacy_fallback"
    assert context.promotion_id is None
    assert context.ranking_run_id == "rank-legacy"
    assert context.embedding_version == "legacy-embedding"
    assert "w.corpus_snapshot_version = ssv.source_snapshot_version" in conn.calls[0][0]
    assert conn.calls[1][1] == ("legacy-snapshot",)


def test_empty_promotion_table_without_legacy_run_raises_not_found(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.serving_context.fetch_latest_public_release_promotion",
        lambda _conn: None,
    )
    conn = _Conn(_Result())

    with pytest.raises(ServingContextNotFoundError) as error:
        resolve_serving_context(conn)  # type: ignore[arg-type]

    assert error.value.ranking_run_id is None
    assert str(error.value) == "No succeeded materialized ranking run is available."


def test_load_serving_context_opens_dict_connection_and_delegates(monkeypatch) -> None:
    connection = object()
    calls: dict[str, Any] = {}
    expected = SimpleNamespace(ranking_run_id="rank-loaded")

    class _ConnectionManager:
        def __enter__(self) -> object:
            return connection

        def __exit__(self, *_args: Any) -> None:
            return None

    def connect(database_url: str, *, row_factory: Any) -> _ConnectionManager:
        calls["connect"] = (database_url, row_factory)
        return _ConnectionManager()

    def resolve(conn: object, **selectors: Any) -> Any:
        calls["resolve"] = (conn, selectors)
        return expected

    monkeypatch.setattr("app.serving_context.database_url_from_env", lambda: "postgresql://api")
    monkeypatch.setattr("app.serving_context.psycopg.connect", connect)
    monkeypatch.setattr("app.serving_context.resolve_serving_context", resolve)

    result = load_serving_context(
        ranking_run_id="rank-loaded",
        corpus_snapshot_version="snapshot-loaded",
        ranking_version="version-loaded",
    )

    assert result is expected
    assert calls["connect"][0] == "postgresql://api"
    assert calls["resolve"] == (
        connection,
        {
            "ranking_run_id": "rank-loaded",
            "corpus_snapshot_version": "snapshot-loaded",
            "ranking_version": "version-loaded",
        },
    )
