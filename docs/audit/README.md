# Audit scripts

## `semantic_coverage_baseline.sql`

Baseline for the **semantic v1 coverage** milestone: one reference `ranking_run_id` (or latest succeeded run), the run’s `corpus_snapshot_version` and `embedding_version`, and tabular counts for `paper_scores` + `embeddings`.

### Before you run

1. **Freeze the reference run** (recommended): in `semantic_coverage_baseline.sql`, edit **once** inside the `params` CTE (`ref_ranking_run_id`) to your chosen `TEXT` id, or leave `NULL` to use latest succeeded by `finished_at`.
2. Ensure `DATABASE_URL` points at the same Postgres the API uses.

### Run

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f docs/audit/semantic_coverage_baseline.sql
```

### Save a milestone snapshot

```bash
mkdir -p docs/audit/out
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f docs/audit/semantic_coverage_baseline.sql -o docs/audit/out/semantic_baseline_$(date +%Y%m%d).txt
```

Keep `docs/audit/out/` out of git if it contains environment-specific paths (add to `.gitignore` if you commit the directory empty, or store artifacts in tickets instead).

### Sections in the output

| Section | Meaning |
|--------|---------|
| `1_reference_run` | Pinned (or latest) succeeded run identifiers |
| `2_included_works_in_snapshot` | Included works count for that snapshot |
| `3_paper_scores_by_family` | Row counts and semantic/bridge/eligibility non-null counts |
| `4_embedding_coverage_run_version` | Share of included works with an `embeddings` row for the run’s `embedding_version` |
| `5_emerging_semantic_null_vs_missing_embedding` | Splits semantic-null emerging rows: missing embedding row vs embedding present but semantic still null |

Interpretation of **semantic null** still requires **run config** (weights): null may be acceptable if semantic weight is zero for that run; otherwise treat as a coverage gap to close in the milestone.
