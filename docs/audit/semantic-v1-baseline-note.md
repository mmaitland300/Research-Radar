# Semantic v1 — baseline coverage note

**Branch:** `semantic-v1-coverage`  
**Purpose:** One short record after the first audit run. Paste numbers from `semantic_coverage_baseline.sql` (or attach `docs/audit/out/*.txt` in the ticket).

## Frozen reference run (fill in)

| Field | Value |
|-------|-------|
| `ranking_run_id` | |
| `ranking_version` | |
| `corpus_snapshot_version` | |
| `embedding_version` | |

## Included scope

| Metric | Value |
|--------|-------|
| Included works (snapshot) | |

## paper_scores (from audit section 3)

Paste the table rows for `emerging` / `bridge` / `undercited` (score_rows, semantic_nonnull_pct, row_exists_semantic_null, etc.).

## Embeddings (section 4)

| Metric | Value |
|--------|-------|
| `pct_with_embedding_row` | |

## Emerging semantic null split (section 5)

| Metric | Value |
|--------|-------|
| `emerging_semantic_null_and_no_embedding_row` | |
| `emerging_semantic_null_but_embedding_row_exists` | |

## One-line verdict

- **Baseline gap is mostly:** [missing embeddings / pipeline not writing semantic / both / neither — edit after audit]

Next: draft numeric acceptance criteria in `docs/ml-r2-execution-plan.md` (Semantic v1 milestone section) from this baseline.
