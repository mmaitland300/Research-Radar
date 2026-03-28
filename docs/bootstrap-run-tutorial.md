# Bootstrap run tutorial (beginner)

A **bootstrap run** is the first real loading pass: OpenAlex is queried for papers from approved venues, raw responses are kept, the policy decides what belongs in the corpus, normalized rows land in Postgres, and artifacts record exactly what happened.

## Before you run

A real bootstrap run needs all of the following:

1. **Postgres running and reachable**  
   For example, start the stack in `docker-compose.yml` (uses `pgvector/pgvector` and applies `infra/db/schema.sql` on first init).

2. **`schema.sql` already applied** on that database  
   If the data directory was created earlier without the schema, apply `infra/db/schema.sql` manually (for example with `psql`) before ingesting.

3. **Connection settings**  
   Set `DATABASE_URL`, or set the usual `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE` variables to match your instance (defaults in the loader align with `docker-compose.yml`: user/db/password `research_radar` on `localhost:5432`).

4. **A polite OpenAlex contact**  
   Set `OPENALEX_MAILTO` to a real email, or pass `--mailto` on the command line. OpenAlex asks for this in the HTTP User-Agent.

## Run the loader

From `services/pipeline` (after `pip install -e .`):

```bash
python -m pipeline.cli bootstrap-run
```

Useful flags:

- `--max-pages-per-source N` - smoke test without pulling every page.
- `--output DIR` - where snapshot metadata and manifests go (default `artifacts`).
- `--raw-root DIR` - where raw OpenAlex **page** JSON files are written (default `artifacts`).
- `--database-url URL` - override Postgres URL for this run.
- `--mailto you@example.com` - override the OpenAlex contact for this run.

## Where outputs go

- **Raw pages** (full API page payloads): under `{raw-root}/{snapshot_version}/{source_slug}/` (for example `artifacts/source-snapshot-20260328-050135/ismir/page-0000.json` when defaults are used).
- **Plan and manifest JSON** (snapshot, ingest run, resolution results, work plan, final manifest): under `{output}/{snapshot_version}/`.

## When something fails

- **If the run fails before a snapshot is created** (for example source resolution or early DB sync), check **`{output}/bootstrap-preflight-failure.json`** for `stage`, `error_message`, and `recorded_at`.
- **If the run fails after a snapshot exists**, look in **`{output}/{snapshot_version}/`** for a `*-failed.json` file next to the ingest metadata, and inspect the **`ingest_runs`** row in Postgres (status, `error_message`, `finished_at`).

## Related commands

- `python -m pipeline.cli bootstrap-plan --resolve-openalex` - write planning artifacts and work-plan URLs using live source resolution (no full ingest).
- `python -m pipeline.cli bootstrap-plan --database-source-ids` - same, but read canonical source IDs from Postgres after a prior successful resolve.

## Mental model (short)

Three layers of truth:

| Layer | Meaning | Examples |
| ----- | ------- | -------- |
| Raw | What OpenAlex returned | Page JSON on disk, `raw_openalex_works` |
| Normalized | What the product queries | `works`, `authors`, `topics`, `citations` |
| Process | How the dataset was produced | `source_snapshot_versions`, `ingest_runs`, `ingest_watermarks`, `snapshot-manifest.json` |

The process layer is what makes runs repeatable and debuggable when you add ranking and explainability later.
