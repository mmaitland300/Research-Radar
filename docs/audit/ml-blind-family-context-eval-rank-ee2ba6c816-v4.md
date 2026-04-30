# Blind-source family-context diagnostic

Read-only diagnostic that evaluates how the heuristic ranking's per-family **context scores and ranks** (carried as worksheet context fields on `ml_blind_snapshot_audit` rows) behave on the blind manual labels. **This is not validation.** Blind rows were drawn from a cluster-stratified blind sample of the corpus snapshot, **not** from any family's top-k ranking, and `family` stays `null` on every row in the dataset.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **label_dataset_path:** `docs/audit/ml-label-dataset-v4.json`
- **label_dataset_version:** `ml-label-dataset-v4`
- **label_dataset_sha256:** `88a3067b48f52b6a99295c51e75da54dd03b2b84bd43b9edc674755a28f92288`
- **review_pool_variant:** `ml_blind_snapshot_audit`
- **generated_at:** `2026-04-30T06:57:31Z`

## Blind row summary

- **Blind rows included (audit_only, run match, after row_id dedupe):** 60
- **Duplicate row_id rows skipped:** 0
- **Rows with any `ranking_context_family_scores_json`:** 60
- **Rows with any `ranking_context_family_ranks_json`:** 60
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
| `bridge` | `good_or_acceptable` | 51 | 9 | 0 | 135.0000 | 168.0000 | -0.0219 | -0.2000 | 0.6275 |
| `bridge` | `surprising_or_useful` | 46 | 14 | 0 | 126.0000 | 169.5000 | -0.0025 | -0.2000 | 0.6413 |
| `bridge` | `bridge_like_yes_or_partial` | 48 | 4 | 8 | 144.5000 | 80.5000 | -0.0447 | 0.2076 | 0.2396 |
| `emerging` | `good_or_acceptable` | 51 | 9 | 0 | 139.0000 | 157.0000 | 0.2354 | 0.1653 | 0.6776 |
| `emerging` | `surprising_or_useful` | 46 | 14 | 0 | 142.0000 | 155.0000 | 0.2426 | 0.1668 | 0.6009 |
| `emerging` | `bridge_like_yes_or_partial` | 48 | 4 | 8 | 146.0000 | 81.0000 | 0.2260 | 0.3322 | 0.1562 |
| `undercited` | `good_or_acceptable` | 51 | 9 | 0 | 52.5000 | *null* | 0.6150 | *null* | *null* |
| `undercited` | `surprising_or_useful` | 46 | 14 | 0 | 52.5000 | *null* | 0.6150 | *null* | *null* |
| `undercited` | `bridge_like_yes_or_partial` | 48 | 4 | 8 | 61.5000 | 48.5000 | 0.6022 | 0.6788 | 0.3500 |

See JSON `metrics.by_family_context` for full counts including `rows_with_family_score` and `rows_with_family_rank`.
