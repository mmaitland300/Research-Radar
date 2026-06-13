"""Add work_source_snapshot_memberships join table.

A work row keeps one canonical home snapshot on works.corpus_snapshot_version.
Snapshot membership rows let the same work appear in additional composed
snapshots (for example a 528 + 20 expanded product pool) without rewriting
canonical work rows or breaking pinned live serving paths.

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-13
"""

from __future__ import annotations

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS work_source_snapshot_memberships (
            work_id BIGINT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
            source_snapshot_version TEXT NOT NULL
                REFERENCES source_snapshot_versions(source_snapshot_version) ON DELETE CASCADE,
            inclusion_status TEXT NOT NULL CHECK (inclusion_status IN ('included', 'excluded')),
            source_slug TEXT REFERENCES source_policies(source_slug),
            added_by_ingest_run_id TEXT REFERENCES ingest_runs(ingest_run_id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (work_id, source_snapshot_version)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_work_snapshot_memberships_snapshot_included
            ON work_source_snapshot_memberships (source_snapshot_version, inclusion_status)
        """
    )
    op.execute(
        """
        INSERT INTO work_source_snapshot_memberships (
            work_id,
            source_snapshot_version,
            inclusion_status,
            source_slug,
            added_by_ingest_run_id
        )
        SELECT
            w.id,
            w.corpus_snapshot_version,
            w.inclusion_status,
            w.source_slug,
            w.last_ingest_run_id
        FROM works w
        ON CONFLICT (work_id, source_snapshot_version) DO NOTHING
        """
    )
    op.execute(
        """
        ALTER TABLE source_snapshot_versions
        DROP CONSTRAINT IF EXISTS source_snapshot_versions_ingest_mode_check
        """
    )
    op.execute(
        """
        ALTER TABLE source_snapshot_versions
        ADD CONSTRAINT source_snapshot_versions_ingest_mode_check
        CHECK (ingest_mode IN (
            'api-bootstrap',
            'api-incremental',
            'snapshot-import',
            'snapshot-compose'
        ))
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS work_source_snapshot_memberships")
