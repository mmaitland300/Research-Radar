# Blind-source family-context diagnostic

Read-only diagnostic that evaluates how the heuristic ranking's per-family **context scores and ranks** (carried as worksheet context fields on `ml_blind_snapshot_audit` rows) behave on the blind manual labels. **This is not validation.** Blind rows were drawn from a cluster-stratified blind sample of the corpus snapshot, **not** from any family's top-k ranking, and `family` stays `null` on every row in the dataset.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **label_dataset_path:** `docs/audit/ml-label-dataset-v5.json`
- **label_dataset_version:** `ml-label-dataset-v5`
- **label_dataset_sha256:** `0dde7a62d4e7d628aa7626f5501c0982603188e1542bc381ec054e615b1ff6d7`
- **review_pool_variant:** `ml_blind_snapshot_audit`
- **generated_at:** `2026-05-13T17:21:23Z`

## Blind row summary

- **Blind rows included (audit_only, run match, after row_id dedupe):** 120
- **Duplicate row_id rows skipped:** 0
- **Rows with any `ranking_context_family_scores_json`:** 120
- **Rows with any `ranking_context_family_ranks_json`:** 120
- **Context family keys seen:** `bridge`, `emerging`, `undercited`
- **All rows have `family=null`:** True

## What this diagnostic answers

For each `(family_context, target)` pair, it reports row counts and how the family's context score/rank distributes across positive vs negative manual labels among the blind sample. AUC is reported **only when both classes exist**, and only as a **diagnostic** of the context score's ordering on this blind label set - not as production-validation evidence.

## What this diagnostic is *not*

- It is **not** validation of the production ranking.
- It does **not** treat blind rows as family-selected ranking outputs.
- It does **not** reassign `family`; rows remain `family=null`.
- It does **not** infer labels from any context field.
- It does **not** support changing production ranking defaults.

## Caveats

- This is not validation.
- Blind rows were not sampled from family top-k rankings.
- Family scores/ranks are context fields, not labels.
- Results must not change production ranking defaults.
- All rows remain audit_only.

## Headline metrics (per family context)

| family_context | target | positive | negative | null | median_rank_pos | median_rank_neg | mean_score_pos | mean_score_neg | diagnostic_auc |
|---|---|---|---|---|---|---|---|---|---|
| `bridge` | `good_or_acceptable` | 110 | 10 | 0 | 131.5000 | 167.0000 | -0.0382 | -0.2000 | 0.6136 |
| `bridge` | `surprising_or_useful` | 101 | 19 | 0 | 129.0000 | 166.0000 | -0.0237 | -0.2000 | 0.6238 |
| `bridge` | `bridge_like_yes_or_partial` | 105 | 7 | 8 | 135.0000 | 76.0000 | -0.0609 | 0.2567 | 0.2422 |
| `emerging` | `good_or_acceptable` | 110 | 10 | 0 | 133.5000 | 163.0000 | 0.2284 | 0.1645 | 0.7355 |
| `emerging` | `surprising_or_useful` | 101 | 19 | 0 | 135.0000 | 153.0000 | 0.2336 | 0.1668 | 0.6175 |
| `emerging` | `bridge_like_yes_or_partial` | 105 | 7 | 8 | 138.0000 | 75.0000 | 0.2195 | 0.3430 | 0.2544 |
| `undercited` | `good_or_acceptable` | 110 | 10 | 0 | 46.0000 | *null* | 0.6431 | *null* | *null* |
| `undercited` | `surprising_or_useful` | 101 | 19 | 0 | 46.0000 | *null* | 0.6431 | *null* | *null* |
| `undercited` | `bridge_like_yes_or_partial` | 105 | 7 | 8 | 47.0000 | 38.5000 | 0.6333 | 0.6894 | 0.2895 |

See JSON `metrics.by_family_context` for full counts including `rows_with_family_score` and `rows_with_family_rank`.
