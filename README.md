# Research Radar

Research Radar is a product-shaped prototype for ranking and explaining papers in a curated MIR + audio-ML slice, with bridge signal currently exposed as diagnostics rather than a validated recommender family.

The V1 thesis is:

> Find emerging and undercited papers in audio ML, while exposing bridge-candidate diagnostics as an experimental bridge review arm. Default readiness and stronger recommender claims require further evidence—scaled evaluation, labeling, and policy. Not bridge weighting alone.

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

## Current status - 2026-04

Research Radar is a deployed, product-shaped prototype. The current public slice includes:

- lexical Search over title + abstract
- materialized Recommended feeds for emerging and undercited families, plus a bridge preview/diagnostics surface
- Paper Detail with metadata, topic labels, ranking placement, and embedding-backed neighbors when `NEXT_PUBLIC_EMBEDDING_VERSION` is pinned
- Trends over the curated corpus (not OpenAlex-wide)
- Evaluation comparing ranked output against citation/date baselines

Current limits: corpus is intentionally narrow (currently TISMIR + JAES), evaluation is proxy-only, and bridge remains experimental.

For a short reviewer-oriented proof map, see `docs/reviewer-brief.md`. The full planning log remains in `docs/roadmap.md`.

**Bootstrap corpus (implemented):** OpenAlex ingest is wired for the venues in `services/pipeline/pipeline/policy.py` (currently **TISMIR** and **JAES**). The broader venue list in `docs/build-brief.md` is product intent; expand `policy.py` when adding sources.

### Embedding versions and the web UI

Multiple rows in `embeddings` can coexist: each vector is keyed by `(work_id, embedding_version)`. Typical workflow:

- Run `embed-works` with one label (e.g. `v1-title-abstract-1536`).
- After text normalization or model changes, run again with a **new** label (e.g. `v1-title-abstract-1536-cleantext`) so Pass 1 / Pass 2 retrieval reviews stay comparable without overwriting prior vectors.

`NEXT_PUBLIC_EMBEDDING_VERSION` selects which label the **paper detail** “Similar papers” block calls (`GET /api/v1/papers/{id}/similar?embedding_version=...`). Match it to the version you are demoing or reviewing so UI and ML1d worksheets do not drift. The API always accepts an explicit `embedding_version` query parameter for scripts and reviews.

## Quickstart vertical slice (works now)

This repo supports a complete local path: bootstrap a small corpus, start the API/web apps, and inspect live Search + Recommended + Trends + Evaluation surfaces backed by Postgres.

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

You can also materialize one embedding per included work from `title + abstract` (stored text; cleantext normalization runs at ingest in `normalize.py` / `openalex_text.py`):

`python -m pipeline.cli embed-works --embedding-version v1-title-abstract-1536`

Use a **distinct** `--embedding-version` string for each comparable retrieval experiment (e.g. cleantext follow-up) so both vector sets remain in Postgres.

### Required env vars

- `DATABASE_URL` or equivalent `PG*` vars (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)
- `OPENALEX_MAILTO`
- `OPENAI_API_KEY` for `embed-works`
- `API_BASE_URL` or `NEXT_PUBLIC_API_BASE_URL` (optional; default API target is `http://localhost:8000`)
- `NEXT_PUBLIC_EMBEDDING_VERSION` (optional; pins which `embedding_version` the Similar papers UI uses; must match a row set you have written — see “Embedding versions and the web UI” above)
- `OPENAI_BASE_URL` (optional; defaults to `https://api.openai.com/v1`)

### What to expect

- `GET /api/v1/papers` returns DB-backed rows from `works` with `inclusion_status='included'`
- `/search` renders real paper rows from the API
- Pipeline unit tests run in CI
- Web build runs in CI

### Pre-merge checks (local)

From the repo root, with Node and (for Python) the same interpreter CI uses:

- `npm run validate:web` — `lint:web` and `build:web`
- `npm run validate:py` — `python -m pytest services/pipeline/tests apps/api/tests -q` (same paths as `.github/workflows/ci.yml`)
- `npm run validate` — both of the above (skips or fails the Python step if `python` is not on `PATH`; run the two commands separately in that case)

### If it fails, check

- `artifacts/bootstrap-preflight-failure.json`
- The snapshot artifact folder under `artifacts/`
- `ingest_runs` in Postgres
- API startup logs for DB connection errors

If you change ranking tables in `infra/db/schema.sql` after the DB was first initialized, recreate the Postgres volume (or apply the DDL manually) so `ranking_runs` and `paper_scores` stay in sync.

For more detailed bootstrap failure checkpoints, see `docs/bootstrap-run-tutorial.md`.

For frozen low-citation candidate semantics (before ranking changes), see `docs/candidate-pool-low-cite.md`.
