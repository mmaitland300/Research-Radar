"""Tests for work_source_snapshot_memberships helpers and compose flow."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.snapshot_membership import (
    SnapshotMembershipError,
    compose_snapshot_from_sources,
    count_included_memberships,
    upsert_work_snapshot_membership,
)


def test_count_included_memberships_uses_membership_table() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (548,)
    assert count_included_memberships(conn, snapshot_version="snap-x") == 548
    sql = conn.execute.call_args[0][0]
    assert "work_source_snapshot_memberships" in sql


def test_compose_snapshot_copies_memberships_without_touching_works() -> None:
    conn = MagicMock()

    def execute(sql: str, params: tuple | None = None):
        compact = " ".join(sql.split())
        result = MagicMock()
        if compact.startswith("SELECT 1 FROM source_snapshot_versions WHERE source_snapshot_version"):
            result.fetchone.return_value = None if params and params[0] == "target-snap" else (1,)
            return result
        if compact.startswith("INSERT INTO source_snapshot_versions"):
            return result
        if compact.startswith("SELECT COUNT(*)") and "work_source_snapshot_memberships" in compact:
            result.fetchone.return_value = (2,)
            return result
        if "FROM work_source_snapshot_memberships" in compact and "WHERE source_snapshot_version" in compact:
            if params and params[0] == "source-a":
                result.fetchall.return_value = [(1, "included", "tismir", "ingest-a")]
            elif params and params[0] == "source-b":
                result.fetchall.return_value = [(2, "included", "ismir", "ingest-b")]
            else:
                result.fetchall.return_value = []
            return result
        if compact.startswith("INSERT INTO work_source_snapshot_memberships"):
            return result
        raise AssertionError(f"unexpected SQL: {compact}")

    conn.execute.side_effect = execute
    result = compose_snapshot_from_sources(
        conn,
        target_snapshot_version="target-snap",
        source_snapshot_versions=("source-a", "source-b"),
        policy_name="research-radar-v1",
        policy_hash="abc123",
    )
    assert result.snapshot_version == "target-snap"
    assert result.membership_count == 2
    assert result.counts_by_source == {"source-a": 1, "source-b": 1}
    sql_calls = " ".join(call.args[0] for call in conn.execute.call_args_list)
    assert "UPDATE works" not in sql_calls


def test_compose_snapshot_rejects_existing_target() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (1,)
    with pytest.raises(SnapshotMembershipError, match="already exists"):
        compose_snapshot_from_sources(
            conn,
            target_snapshot_version="existing-snap",
            source_snapshot_versions=("source-a",),
            policy_name="research-radar-v1",
            policy_hash="abc123",
        )


def test_upsert_work_snapshot_membership_uses_conflict_update() -> None:
    conn = MagicMock()
    upsert_work_snapshot_membership(
        conn,
        work_id=7,
        source_snapshot_version="snap-x",
        source_slug="ismir",
        added_by_ingest_run_id="ingest-1",
    )
    sql = conn.execute.call_args[0][0]
    assert "ON CONFLICT (work_id, source_snapshot_version)" in sql


def test_run_compose_snapshot_from_cli_writes_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from pipeline import snapshot_membership as sm

    monkeypatch.setattr(
        sm,
        "compose_snapshot_from_sources",
        lambda conn, **kwargs: sm.ComposeSnapshotResult(
            snapshot_version=kwargs["target_snapshot_version"],
            source_snapshot_versions=tuple(kwargs["source_snapshot_versions"]),
            membership_count=548,
            counts_by_source={"a": 528, "b": 20},
        ),
    )

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def commit(self) -> None:
            return None

    monkeypatch.setattr(sm.psycopg, "connect", lambda _dsn, autocommit=False: _Conn())

    out = tmp_path / "compose.json"
    md = tmp_path / "compose.md"
    summary = sm.run_compose_snapshot_from_cli(
        snapshot_version="target-snap",
        from_snapshots=["source-a", "source-b"],
        output_path=out,
        markdown_output_path=md,
        database_url="postgresql://example",
    )
    assert summary["membership_count"] == 548
    assert json.loads(out.read_text(encoding="utf-8"))["membership_count"] == 548
    assert "target-snap" in md.read_text(encoding="utf-8")
