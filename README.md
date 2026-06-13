# Research Radar

[![CI](https://github.com/mmaitland300/Research-Radar/actions/workflows/ci.yml/badge.svg)](https://github.com/mmaitland300/Research-Radar/actions/workflows/ci.yml)
[![CodeQL](https://github.com/mmaitland300/Research-Radar/actions/workflows/codeql.yml/badge.svg)](https://github.com/mmaitland300/Research-Radar/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Research Radar is a deployed prototype for exploring and ranking papers in a
curated MIR and audio-ML corpus. It includes search, recommended feeds, paper
detail pages, trends, and evaluation views. Bridge diagnostics are experimental,
and the project is not presented as a validated recommender system.

## Start Here

- Try the live app: [radar.mmaitland.dev](https://radar.mmaitland.dev).
- Run the no-key demo with `npm run demo:local` after installing the API and web
  dependencies.
- See [ARCHITECTURE.md](ARCHITECTURE.md) for the system shape in one page.
- Inspect the current evaluation boundary in [EVALUATION.md](EVALUATION.md).
- Read the [public roadmap](docs/public-roadmap.md) and
  release notes:
  [v0.1.0 public baseline](docs/releases/v0.1.0-public-baseline.md) and
  [v0.2.0 curated surface](docs/releases/v0.2.0.md).
- Check the ranked API and fixture-mode tests if you want to see the core
  behavior under test.
- Main limitation: the corpus is intentionally narrow, and evaluation is still
  proxy/single-reviewer only.

## What It Is

- A Next.js + FastAPI + Python pipeline project backed by PostgreSQL and
  pgvector.
- A tool for finding emerging and undercited audio-ML papers in a deliberately
  narrow corpus.
- An explainable ranking prototype, not a validated recommender system.

## Status

Current status: deployed working prototype with a curated public surface.

Current curated-surface milestone: `v0.2.0` documents the June 2026 cleanup of
the public repo surface, route/CLI structure, and release story. Public
baseline: `v0.1.0-public-baseline` remains the archived review surface, release
smoke path, and known limits from the first public baseline.

Implemented public surfaces:

- Search over title and abstract text
- Recommended feeds for emerging and undercited papers, plus bridge
  preview/diagnostics
- Paper Detail with metadata, topic labels, ranking placement, and optional similar papers
- Trends over the curated corpus
- Evaluation comparing ranked output against citation/date baselines

Current corpus scope is intentionally narrow: TISMIR + JAES. `Idea Graph` is
explicitly out of scope for V1.

## Demo

- Live app: [radar.mmaitland.dev](https://radar.mmaitland.dev)
- Recommended emerging papers: [recommended?family=emerging](https://radar.mmaitland.dev/recommended?family=emerging)
- Paper detail example: [OpenAlex W3093121331](https://radar.mmaitland.dev/papers/https%3A%2F%2Fopenalex.org%2FW3093121331)
- Trends: [trends](https://radar.mmaitland.dev/trends)
- Evaluation: [evaluation?family=emerging](https://radar.mmaitland.dev/evaluation?family=emerging)

## Live App vs Archived Baseline

Research Radar has a live demo surface and archived evaluation records. Read
them as different kinds of evidence:

| Surface | Use it for | Stability |
| --- | --- | --- |
| Live app | Interactive walkthrough of the current deployed UI, ranking pages, and visible run metadata | Moving surface; deployment config and data pins can change |
| Archived baseline | Stable reference for documented evaluation boundaries and single-reviewer audit notes | Frozen on `2026-04-25`; use for repeatable claims and comparisons |
| No-key fixture demo | Local UI/API walkthrough without Postgres, OpenAlex, or OpenAI credentials | Deterministic toy data; not live ranking data or model validation |

For exact review or comparison work, use the pinned baseline in
[EVALUATION.md](EVALUATION.md) or add a `ranking_run_id` parameter to inspect a
specific materialized run.

## No-Key Local Demo

Use this path when you want to inspect the UI/API quickly without Postgres,
pgvector, OpenAlex, or OpenAI credentials. It starts the same FastAPI and Next.js
apps, but sets `RESEARCH_RADAR_DATA_MODE=fixture` so API responses come from a
tiny checked-in fixture corpus.

```bash
pip install -e ./apps/api
npm install
npm run demo:local
```

Open `http://localhost:3000/search`, then try Recommended, Trends, Evaluation,
and a paper detail page. Fixture mode is for quick local walkthroughs only; it
is not live ranking data and should not be cited as model validation. The
launcher also pins fixture ranking and embedding versions so the UI stays
deterministic. For API-shape compatibility, fixture mode accepts parameters such as
`ranking_run_id`, `corpus_snapshot_version`, and `cluster_version`, but it always
resolves them to the checked-in fixture run metadata.

## Full Postgres Run Locally

This path bootstraps a small corpus, starts the API and web apps, and lets you
inspect Search, Recommended, Trends, and Evaluation backed by Postgres.

```bash
docker compose up -d
# schema is auto-applied from infra/db/schema.sql on first init

pip install -r infra/db/requirements.txt
cd infra/db && alembic upgrade head && cd ../..
# Required before corpus-v2 compose/embed/cluster/rank commands: revision 0002
# adds work_source_snapshot_memberships.

pip install -e ./services/pipeline
pip install -e ./apps/api
npm install

python -m pipeline.cli bootstrap-run --max-pages-per-source 1 --mailto "$OPENALEX_MAILTO"
uvicorn app.main:app --reload --app-dir apps/api
npm run dev:web
```

Open `http://localhost:3000/search`.

### Required env vars for the full Postgres path

- `DATABASE_URL` or equivalent `PG*` vars (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)
- `OPENALEX_MAILTO`
- `OPENAI_API_KEY` for `embed-works`
- `API_BASE_URL` or `NEXT_PUBLIC_API_BASE_URL` (optional; default API target is `http://localhost:8000`)
- `NEXT_PUBLIC_EMBEDDING_VERSION` (optional; pins which `embedding_version` the Similar papers UI uses)
- `OPENAI_BASE_URL` (optional; defaults to `https://api.openai.com/v1`)

## Evaluation and Tests

- Evaluation status and current boundaries: [EVALUATION.md](EVALUATION.md)
- Detailed technical brief: [docs/reviewer-brief.md](docs/reviewer-brief.md)
- Ranked recommendation API tests: [apps/api/tests/test_recommendations_ranked.py](apps/api/tests/test_recommendations_ranked.py)
- Evaluation API tests: [apps/api/tests/test_evaluation_compare.py](apps/api/tests/test_evaluation_compare.py)
- No-key fixture demo tests: [apps/api/tests/test_demo_fixture_mode.py](apps/api/tests/test_demo_fixture_mode.py)
- Ranked explanation surface: [apps/web/app/recommended/page.tsx](apps/web/app/recommended/page.tsx)
- CI validates the web build and pipeline/API tests through `npm run validate`.

## Limits

- The corpus is intentionally narrow and currently wired to TISMIR + JAES.
- Evaluation is proxy-only, not a human-labeled relevance benchmark.
- Bridge remains experimental; diagnostics should not be read as
  default-readiness claims.
- Embedding-backed similar papers require stored vectors and a matching
  `NEXT_PUBLIC_EMBEDDING_VERSION`.
- The broader venue list in [docs/build-brief.md](docs/build-brief.md) is
  product intent; expand `services/pipeline/pipeline/policy.py` when adding
  sources.

## Contributing And Security

Focused issue reports and small PRs are welcome for reproducible API/ranking
bugs, fixture demo issues, tests, and documentation corrections. See
[CONTRIBUTING.md](CONTRIBUTING.md).

Do not post API keys, database URLs, `.env` files, private database dumps, or
unredacted provider payloads in issues or PRs. See [SECURITY.md](SECURITY.md).

## Repo Layout

- `apps/web` - Next.js frontend for search, recommendations, trends, and
  evaluation
- `apps/api` - FastAPI service for metadata, rankings, and explanation endpoints
- `services/pipeline` - Python ETL, bootstrap ingest planning, and ranking jobs
- `infra/db` - PostgreSQL + pgvector schema and Alembic migrations
- `docs` - reviewer brief, operator guides, and internal planning notes; see
  the [documentation map](docs/README.md)

The full ML experiment and rollout-review record is preserved on the
`archive/ml-governance-audit` branch; `main` keeps only the pinned model
artifacts the deployed scorers load. See
[docs/releases/v0.2.0.md](docs/releases/v0.2.0.md) for the curated-surface
milestone.

## Quickstart Details

After ingest, you can persist a Step-2 stub ranking run, which writes
`ranking_runs` and `paper_scores`:

```bash
python -m pipeline.cli ranking-run --ranking-version v0-heuristic-no-embeddings
```

You can also materialize one embedding per included work from stored
`title + abstract` text:

```bash
python -m pipeline.cli embed-works --embedding-version v1-title-abstract-1536
```

Use a distinct `--embedding-version` string for each comparable retrieval
experiment so vector sets remain side by side in Postgres.

### What to expect

- `GET /api/v1/papers` returns DB-backed rows from `works` with `inclusion_status='included'`
- `/search` renders real paper rows from the API
- Pipeline unit tests run in CI
- Web build runs in CI

### Pre-merge checks

From the repo root, with Node and the same Python interpreter CI uses:

- `npm run validate:web` - `lint:web` and `build:web`
- `npm run validate:py` - `python -m pytest services/pipeline/tests apps/api/tests -q`
- `npm run validate` - both of the above

### If it fails, check

- `artifacts/bootstrap-preflight-failure.json`
- The snapshot artifact folder under `artifacts/`
- `ingest_runs` in Postgres
- API startup logs for DB connection errors

Schema changes after the first init are applied with Alembic migrations
(`alembic upgrade head` from `infra/db/`); see
[infra/db/README.md](infra/db/README.md).

For more detailed bootstrap failure checkpoints, see [docs/bootstrap-run-tutorial.md](docs/bootstrap-run-tutorial.md).

For frozen low-citation candidate semantics before ranking changes, see [docs/candidate-pool-low-cite.md](docs/candidate-pool-low-cite.md).

## Project Scope

The V1 target is:

> Find emerging and undercited papers in audio ML, while keeping
> bridge-candidate diagnostics as an experimental analysis lane. Default bridge
> behavior would need a larger corpus, a labeling protocol, and documented
> success criteria.

V1 is scoped to `MIR + audio representation learning`, with `neural audio
effects` and `music/audio generation` deferred to a controlled edge slice in
`V1.1` unless a paper clearly connects back to the core corpus.

## Product Surfaces

- `Search`
- `Recommended`
- `Paper Detail`
- `Trends`
- `Evaluation`

## Engineering Principles

- Ranking is the product, not graph visualization.
- Every recommendation should be explainable with per-signal breakdowns.
- Bootstrap the curated corpus through the OpenAlex API before any
  snapshot-scale ingestion.
- Raw OpenAlex payloads and normalized rows are both retained.
- Snapshot versions, ingest runs, and watermarks are first-class state.
- Evaluation is included from the first usable web version instead of being postponed.

## Embedding Versions and the Web UI

Multiple rows in `embeddings` can coexist: each vector is keyed by
`(work_id, embedding_version)`. Typical workflow:

- Run `embed-works` with one label, such as `v1-title-abstract-1536`.
- After text normalization or model changes, run again with a new label, such as
  `v1-title-abstract-1536-cleantext`, so retrieval reviews stay comparable
  without overwriting prior vectors.

`NEXT_PUBLIC_EMBEDDING_VERSION` selects which label the Paper Detail "Similar
papers" block calls through
`GET /api/v1/papers/{id}/similar?embedding_version=...`. Match it to the
version you are demoing or reviewing so the UI and ML worksheets do not drift.
The API always accepts an explicit `embedding_version` query parameter for
scripts and reviews.
