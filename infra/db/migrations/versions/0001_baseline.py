"""Baseline: frozen schema.sql snapshot.

Executes infra/db/schema.sql verbatim. Every statement in the baseline
uses IF NOT EXISTS, so this revision is safe both on an empty database
and on a database that was initialised from schema.sql directly (the
Docker Compose init path).

Revision ID: 0001
Revises:
Create Date: 2026-06-12
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

_SCHEMA_SQL = Path(__file__).resolve().parents[2] / "schema.sql"


def upgrade() -> None:
    op.execute(_SCHEMA_SQL.read_text(encoding="utf-8"))


def downgrade() -> None:
    raise NotImplementedError(
        "The baseline cannot be downgraded; drop and recreate the database instead."
    )
