# Research Radar

Research Radar is a ranking and explainability product for emerging and bridge papers in audio ML.

The V1 thesis is:

> Find emerging and bridge papers in audio ML before they become default citations.

V1 is intentionally scoped to `MIR + audio representation learning`, with `neural audio effects` and `music/audio generation` deferred to a controlled edge slice in `V1.1` unless a paper clearly connects back to the core corpus.

## Repo layout

- `apps/web` - Next.js frontend for search, recommendations, trends, and evaluation
- `apps/api` - FastAPI service for metadata, rankings, and explanation endpoints
- `services/pipeline` - Python ETL, bootstrap ingest planning, and ranking jobs
- `infra/db` - PostgreSQL + pgvector schema
- `docs` - build brief, roadmap, and implementation notes

## Product pages in V1

- `Search`
- `Recommended`
- `Paper Detail`
- `Trends`
- `Evaluation`

`Idea Graph` is explicitly out of scope for V1.

## Engineering principles

- Ranking is the product, not graph visualization.
- Every recommendation should be explainable with per-signal breakdowns.
- Bootstrap the curated corpus through the OpenAlex API before any snapshot-scale ingestion.
- Raw OpenAlex payloads and normalized rows are both retained.
- Snapshot versions, ingest runs, and watermarks are first-class state.
- Evaluation ships with the MVP instead of being postponed.

## Current status

The repository contains the initial product scaffold plus an API-bootstrap-ready corpus policy, snapshot/run manifests, raw-payload retention helpers, normalization helpers, and a starter schema for reproducible ingest.

## Quickstart vertical slice (works now)

This repo already supports one complete path: bootstrap a small corpus, start the API, open the web search page, and see live papers served from Postgres.

### Fast path commands

```bash
docker compose up -d
# schema is auto-applied from infra/db/schema.sql on first init

pip install -e ./services/pipeline
pip install -e ./apps/api
npm install

python -m pipeline.cli bootstrap-run --max-pages-per-source 1 --mailto "$OPENALEX_MAILTO"
uvicorn app.main:app --reload --app-dir apps/api
npm run dev:web
```

Open `http://localhost:3000/search`.

After ingest, you can persist a Step-2 stub ranking run (writes `ranking_runs` + `paper_scores`):

`python -m pipeline.cli ranking-run --ranking-version v0-heuristic-no-embeddings`

You can also materialize one embedding per included work from `title + abstract`:

`python -m pipeline.cli embed-works --embedding-version v1-title-abstract-1536`

### Required env vars

- `DATABASE_URL` or equivalent `PG*` vars (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)
- `OPENALEX_MAILTO`
- `OPENAI_API_KEY` for `embed-works`
- `API_BASE_URL` or `NEXT_PUBLIC_API_BASE_URL` (optional; default API target is `http://localhost:8000`)
- `OPENAI_BASE_URL` (optional; defaults to `https://api.openai.com/v1`)

### What to expect

- `GET /api/v1/papers` returns DB-backed rows from `works` with `inclusion_status='included'`
- `/search` renders real paper rows from the API
- Pipeline unit tests run in CI
- Web build runs in CI

### If it fails, check

- `artifacts/bootstrap-preflight-failure.json`
- The snapshot artifact folder under `artifacts/`
- `ingest_runs` in Postgres
- API startup logs for DB connection errors

If you change ranking tables in `infra/db/schema.sql` after the DB was first initialized, recreate the Postgres volume (or apply the DDL manually) so `ranking_runs` and `paper_scores` stay in sync.

For more detailed bootstrap failure checkpoints, see `docs/bootstrap-run-tutorial.md`.
