# Recommendation review worksheet (human labels)

## What this is

This is a **human-review scaffold** for one **pinned** materialized `ranking_run_id`. It is **not** a benchmark, validation study, or claim of model quality. Labels exist to support **structured judgment** and future analysis once a complete pass exists.

- Labels are only meaningful **for the worksheet’s run and corpus snapshot**; do not merge ad hoc with other runs without a documented plan.
- **Precision@k** (or similar) comparisons should be added **only after** at least one full labeled pass exists for a defined review protocol.
- The Evaluation page’s **proxy/distributional** checks remain **not** human-relevance tests; this rubric is **independent** of those guards.

## Generating a worksheet (pipeline)

From the pipeline package directory (or with `PYTHONPATH` set to the pipeline package root), after `DATABASE_URL` (or `PG*`) points at a database that has the run:

```bash
python -m pipeline.cli recommendation-review-worksheet \
  --ranking-run-id <RUN_ID> \
  --family <emerging|bridge|undercited> \
  --limit 20 \
  --output docs/audit/manual-review/<name>.csv
```

Optional: `--database-url` to override the DSN. The run must be **`succeeded`**; failed or missing runs are rejected. See `docs/audit/manual-review/.gitkeep` for where checked-in examples may live (optional; paths are your choice).

## `bridge_eligible` in the CSV (normative)

- PostgreSQL `TRUE` → `true` (lowercase)  
- PostgreSQL `FALSE` → `false` (lowercase)  
- SQL **NULL** → **empty** cell in the CSV (not the literal `null`, not `TRUE`/`FALSE`)

## Reviewer columns (to fill in)

| Column | Allowed values | Notes |
|--------|----------------|--------|
| `relevance_label` | `good`, `acceptable`, `miss`, `irrelevant` | Fit to the product’s information need for this list row. |
| `novelty_label` | `obvious`, `useful`, `surprising`, `not_useful` | For “new to me” / surprise vs obvious follow-on. |
| `bridge_like_label` | `yes`, `partial`, `no`, `not_applicable` | Most useful on **bridge**-family rows; for **emerging** or **undercited**, `not_applicable` is often appropriate. |
| `reviewer_notes` | free text | Short; link to paper id in external notes if needed. |

Leave all four **blank** until a reviewer has assigned labels (the generator starts them empty).

## Provenance (materialized fields)

The CSV repeats **provenance** on every row: `ranking_run_id`, `ranking_version`, `corpus_snapshot_version`, `embedding_version`, `cluster_version` (from `ranking_runs.config_json.clustering_artifact.cluster_version` when present; otherwise **blank**), `family`, and list **`rank`**.

`bridge_signal_json` is **not** exported; only precomputed **scores and eligibility** appear.

## `bridge_like_label` scope

- **bridge** family: this label is central for boundary-like judgment.
- **Other families:** prefer **`not_applicable`** unless the reviewer is explicitly doing a cross-family bridge judgment task.

## What this does *not* prove

- It does not prove **validated** recommendation quality or **retrieval accuracy**.
- It does not **validate** bridge weighting, neighbors, or clustering; it is input to **qualitative and later metric** work.
