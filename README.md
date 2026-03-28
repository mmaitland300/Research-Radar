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

### Running a real bootstrap load

Prerequisites: Postgres up with `infra/db/schema.sql` applied, `DATABASE_URL` (or matching `PG*` variables), and `OPENALEX_MAILTO` or `--mailto` for OpenAlex. Then from `services/pipeline`: `python -m pipeline.cli bootstrap-run` (see `docs/bootstrap-run-tutorial.md` for paths, flags, and failure checkpoints: `bootstrap-preflight-failure.json` vs snapshot folder / `ingest_runs`).
