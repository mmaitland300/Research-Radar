"""Snapshot membership: many-to-many between works and source snapshots.

Each work row still has a canonical home snapshot on works.corpus_snapshot_version
(for backward-compatible live serving). Additional snapshot views are expressed
through work_source_snapshot_memberships so composed product snapshots can
reference existing pinned works without moving rows.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.policy import CorpusPolicy

INGEST_MODE_COMPOSE = "snapshot-compose"


class SnapshotMembershipError(RuntimeError):
    def __init__(self, message: str, *, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ComposeSnapshotResult:
    snapshot_version: str
    source_snapshot_versions: tuple[str, ...]
    membership_count: int
    counts_by_source: dict[str, int]


def count_included_memberships(conn: psycopg.Connection, *, snapshot_version: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM work_source_snapshot_memberships
        WHERE source_snapshot_version = %s
          AND inclusion_status = 'included'
        """,
        (snapshot_version,),
    ).fetchone()
    return int(row[0] or 0) if row is not None else 0


def assert_snapshot_exists(conn: psycopg.Connection, snapshot_version: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (snapshot_version,),
    ).fetchone()
    if row is None:
        raise SnapshotMembershipError(f"snapshot_version not found: {snapshot_version}", code=2)


def upsert_work_snapshot_membership(
    conn: psycopg.Connection,
    *,
    work_id: int,
    source_snapshot_version: str,
    inclusion_status: str = "included",
    source_slug: str | None = None,
    added_by_ingest_run_id: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO work_source_snapshot_memberships (
            work_id,
            source_snapshot_version,
            inclusion_status,
            source_slug,
            added_by_ingest_run_id
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (work_id, source_snapshot_version) DO UPDATE SET
            inclusion_status = EXCLUDED.inclusion_status,
            source_slug = COALESCE(EXCLUDED.source_slug, work_source_snapshot_memberships.source_slug),
            added_by_ingest_run_id = COALESCE(
                EXCLUDED.added_by_ingest_run_id,
                work_source_snapshot_memberships.added_by_ingest_run_id
            )
        """,
        (work_id, source_snapshot_version, inclusion_status, source_slug, added_by_ingest_run_id),
    )


def latest_snapshot_with_included_memberships(conn: psycopg.Connection) -> str | None:
    row = conn.execute(
        """
        SELECT ssv.source_snapshot_version
        FROM source_snapshot_versions ssv
        WHERE EXISTS (
            SELECT 1
            FROM work_source_snapshot_memberships wssm
            WHERE wssm.source_snapshot_version = ssv.source_snapshot_version
              AND wssm.inclusion_status = 'included'
        )
        ORDER BY ssv.created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return str(row[0])


def compose_snapshot_from_sources(
    conn: psycopg.Connection,
    *,
    target_snapshot_version: str,
    source_snapshot_versions: Sequence[str],
    policy_name: str,
    policy_hash: str,
    note: str | None = None,
) -> ComposeSnapshotResult:
    target_snapshot_version = (target_snapshot_version or "").strip()
    if not target_snapshot_version:
        raise SnapshotMembershipError("--snapshot-version is required", code=2)
    sources = tuple(dict.fromkeys(str(v).strip() for v in source_snapshot_versions if str(v).strip()))
    if not sources:
        raise SnapshotMembershipError("at least one --from-snapshot is required", code=2)

    existing_target = conn.execute(
        "SELECT 1 FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (target_snapshot_version,),
    ).fetchone()
    if existing_target is not None:
        raise SnapshotMembershipError(
            f"target snapshot already exists: {target_snapshot_version}",
            code=2,
        )

    for source in sources:
        assert_snapshot_exists(conn, source)

    conn.execute(
        """
        INSERT INTO source_snapshot_versions (
            source_snapshot_version, policy_name, policy_hash, ingest_mode, note, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            target_snapshot_version,
            policy_name,
            policy_hash,
            INGEST_MODE_COMPOSE,
            note or f"Composed from: {', '.join(sources)}",
            datetime.now(UTC),
        ),
    )

    counts_by_source: dict[str, int] = {}
    total_inserted = 0
    for source in sources:
        rows = conn.execute(
            """
            SELECT work_id, inclusion_status, source_slug, added_by_ingest_run_id
            FROM work_source_snapshot_memberships
            WHERE source_snapshot_version = %s
              AND inclusion_status = 'included'
            ORDER BY work_id ASC
            """,
            (source,),
        ).fetchall()
        source_count = 0
        for work_id, inclusion_status, source_slug, added_by in rows:
            conn.execute(
                """
                INSERT INTO work_source_snapshot_memberships (
                    work_id, source_snapshot_version, inclusion_status, source_slug, added_by_ingest_run_id
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (work_id, source_snapshot_version) DO NOTHING
                """,
                (work_id, target_snapshot_version, inclusion_status, source_slug, added_by),
            )
            source_count += 1
            total_inserted += 1
        counts_by_source[source] = source_count

    membership_count = count_included_memberships(conn, snapshot_version=target_snapshot_version)
    return ComposeSnapshotResult(
        snapshot_version=target_snapshot_version,
        source_snapshot_versions=sources,
        membership_count=membership_count,
        counts_by_source=counts_by_source,
    )


def run_compose_snapshot_from_cli(
    *,
    snapshot_version: str,
    from_snapshots: Sequence[str],
    output_path: Path,
    markdown_output_path: Path,
    database_url: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    policy = CorpusPolicy()
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, autocommit=False) as conn:
        result = compose_snapshot_from_sources(
            conn,
            target_snapshot_version=snapshot_version,
            source_snapshot_versions=from_snapshots,
            policy_name=policy.name,
            policy_hash=policy.policy_hash,
            note=note,
        )
        conn.commit()

    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_version": result.snapshot_version,
        "source_snapshot_versions": list(result.source_snapshot_versions),
        "membership_count": result.membership_count,
        "counts_by_source": result.counts_by_source,
        "policy_reference": {"name": policy.name, "policy_hash": policy.policy_hash},
        "ingest_mode": INGEST_MODE_COMPOSE,
        "note": note,
        "next_step": (
            "Run corpus-v2-embed for rows missing the target embedding_version, then cluster-works "
            "and ranking-run against this snapshot_version. Live pinned serving snapshots are unchanged."
        ),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(_render_compose_markdown(summary), encoding="utf-8", newline="\n")
    return summary


def _render_compose_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Snapshot membership compose summary",
        "",
        f"- **snapshot_version:** `{summary.get('snapshot_version')}`",
        f"- **membership_count:** `{summary.get('membership_count')}`",
        f"- **source snapshots:** `{summary.get('source_snapshot_versions')}`",
        "",
        "## Counts by source",
        "",
    ]
    for source, count in sorted((summary.get("counts_by_source") or {}).items()):
        lines.append(f"- **{source}:** `{count}`")
    lines.extend(["", "## Next step", "", str(summary.get("next_step") or ""), ""])
    return "\n".join(lines).rstrip() + "\n"


__all__ = [
    "ComposeSnapshotResult",
    "SnapshotMembershipError",
    "assert_snapshot_exists",
    "compose_snapshot_from_sources",
    "count_included_memberships",
    "latest_snapshot_with_included_memberships",
    "run_compose_snapshot_from_cli",
    "upsert_work_snapshot_membership",
]
