# Semantic v1 - baseline coverage note

**Branch:** `semantic-v1-coverage`  
**Purpose:** First recorded audit output for the semantic v1 milestone.

## Frozen reference run

| Field | Value |
|-------|-------|
| `ranking_run_id` | `rank-d7f3d82d05` |
| `ranking_version` | `bridge-v2-nm1-zero-r2-k6-20260407` |
| `corpus_snapshot_version` | `source-snapshot-20260329-170012` |
| `embedding_version` | `v1-title-abstract-1536-cleantext-r2` |

## Included scope

| Metric | Value |
|--------|-------|
| Included works (snapshot) | `38` |

## `paper_scores` by family

| Family | score_rows | semantic_nonnull | semantic_nonnull_pct | bridge_score_nonnull | bridge_eligible_nonnull | semantic_null_rows | row_exists_semantic_null |
|--------|------------|------------------|----------------------|----------------------|-------------------------|--------------------|--------------------------|
| `bridge` | `38` | `0` | `0.00%` | `38` | `38` | `38` | `38` |
| `emerging` | `38` | `0` | `0.00%` | `0` | `0` | `38` | `38` |
| `undercited` | `35` | `0` | `0.00%` | `0` | `0` | `35` | `35` |

## Embeddings

| Metric | Value |
|--------|-------|
| `pct_with_embedding_row` | `100.00%` |

## Emerging semantic null split

| Metric | Value |
|--------|-------|
| `emerging_semantic_null_and_no_embedding_row` | `0` |
| `emerging_semantic_null_but_embedding_row_exists` | `38` |

## One-line verdict

- **Baseline gap is mostly:** pipeline not writing semantic for this run. Embedding coverage is complete (`38 / 38`, `100.00%`), but semantic is null on every scored row and all `38` emerging semantic-null rows still have an embedding row.

Next: implement semantic score population on this branch, rerun the audit against a new succeeded `ranking_version`, and compare the section 3 + section 5 deltas.
