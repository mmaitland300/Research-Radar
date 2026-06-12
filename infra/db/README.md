# Database schema and migrations

PostgreSQL 16 with the [pgvector](https://github.com/pgvector/pgvector)
extension. Two files matter here:

- `schema.sql` - the frozen baseline snapshot of the schema as of the
  v0.1.x corpus. Docker Compose applies it automatically when the
  `postgres-data` volume is first created.
- `migrations/` - Alembic migrations. These are the source of truth for all
  schema changes going forward; `schema.sql` is no longer edited.

## Strategy

Before June 2026 the project had no migration story: schema changes meant
recreating the Docker volume and re-running the bootstrap ingest. That was
acceptable while the database was a disposable cache of OpenAlex data, but
ranking runs, embeddings, and manual-review provenance now make the database
stateful enough that destructive resets are a real cost.

The rules:

1. `schema.sql` is frozen as the baseline. The first migration
   (`0001_baseline`) executes it verbatim; because every statement in it uses
   `IF NOT EXISTS`, the baseline is safe to run against both empty databases
   and databases that were created from `schema.sql` directly.
2. Every schema change after the baseline is a new Alembic revision under
   `migrations/versions/`. Do not edit `schema.sql` or past revisions.
3. Migrations run from a repo checkout (developer machine or CI), not from
   the API container. The deployed API never owns DDL.

## Running migrations

Install the tooling (separate from the pipeline package - it is operational,
not a runtime dependency):

```bash
pip install -r infra/db/requirements.txt
```

Alembic reads the database URL from `DATABASE_URL`, falling back to the
standard `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` / `PGDATABASE`
variables and finally to the local Docker Compose defaults - the same
resolution order the pipeline uses.

```bash
cd infra/db

# Fresh or existing database: bring it to the latest revision.
alembic upgrade head

# Inspect state.
alembic current
alembic history
```

### Adopting an existing database

A database that was initialised from `schema.sql` (the Docker Compose init
path) already matches the baseline. `alembic upgrade head` is still the right
command: revision `0001_baseline` is a no-op against it thanks to
`IF NOT EXISTS`, and Alembic records the version so future revisions apply
cleanly. If you prefer to skip the no-op execution, `alembic stamp 0001`
marks the baseline as applied without running it.

### Adding a new migration

```bash
cd infra/db
alembic revision -m "add <thing>"
# edit the generated file under migrations/versions/
alembic upgrade head
```

Keep revisions small and reversible where practical; write an explicit
`downgrade()` or raise `NotImplementedError` with a reason. CI runs
`alembic upgrade head` against a clean pgvector Postgres on every push, so a
broken revision fails the build.
