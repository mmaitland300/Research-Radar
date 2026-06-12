"""Alembic environment for Research Radar.

Resolves the database URL the same way the pipeline does: DATABASE_URL
first, then PG* variables, then the local Docker Compose defaults.
There are no SQLAlchemy models, so autogenerate is not used; migrations
are written by hand against the baseline in infra/db/schema.sql.
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        host = os.environ.get("PGHOST", "localhost")
        port = os.environ.get("PGPORT", "5432")
        user = os.environ.get("PGUSER", "research_radar")
        password = os.environ.get("PGPASSWORD", "research_radar")
        db = os.environ.get("PGDATABASE", "research_radar")
        url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    # SQLAlchemy needs the psycopg (v3) driver spelled out.
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def run_migrations_offline() -> None:
    context.configure(url=_database_url(), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_database_url())
    with engine.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
