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

## Summarizing a completed worksheet (labels only)

After every data row has valid `relevance_label`, `novelty_label`, and `bridge_like_label` (see table below), validate and write a JSON summary (human labels and simple row-level metrics only; no model inference):

```bash
cd services/pipeline
python -m pipeline.cli recommendation-review-summary \
  --input docs/audit/manual-review/bridge_RUN.csv \
  --output docs/audit/manual-review/bridge_RUN_summary.json
```

- **Default behavior:** if any label is blank or not in the allowed set, the command prints which **data row** (1-based, after the header), **`paper_id`**, and **column** failed, and exits with code **2**. No output JSON is written.
- **`--allow-incomplete`:** still writes JSON with `is_complete: false`, includes a **warnings** entry, and is only for triage — **do not** treat metrics as a clean score until you fix labels and re-run without this flag. In triage mode, `bridge_like_yes_or_partial_share` still applies the usual denominator rule over **all** rows (any value other than `not_applicable` counts), so blank or invalid `bridge_like_label` cells can skew that metric until labels are fixed.
- **`--markdown-output PATH`:** optional short Markdown alongside the JSON.

**What the JSON metrics mean (all numerators use the filled worksheet only):**

| Field | Definition |
|-------|------------|
| `precision_at_k_good_only` | `count(relevance_label == good) / row_count` |
| `precision_at_k_good_or_acceptable` | `count(relevance_label in {good, acceptable}) / row_count` |
| `bridge_like_yes_or_partial_share` | `count(bridge_like_label in {yes, partial})` divided by rows with `bridge_like_label != not_applicable`; if that denominator is 0, the value is JSON **`null`**. |
| `surprising_or_useful_share` | `count(novelty_label in {surprising, useful}) / row_count` |

**Meaning and limits:** these numbers are **only** meaningful for analysis after a **complete** labeled pass on a **single** review intent (and ideally one pinned run and snapshot). They are **not** a benchmark or proof of production retrieval quality until the **review protocol**, **corpus scope**, and **k** are fixed and documented. Smoke or demo label sets should not be cited as validation of ML ranking or bridge behavior.

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
