# Text Baseline Cross-Pool (ml-text-baseline-cross-pool-v8)

Offline source-transfer diagnostic over frozen labeled text embeddings and ml-label-dataset-v8 labels.

## Inputs

- **embeddings:** `docs/audit/ml-labeled-text-embeddings-v3.json`
- **embeddings_sha256:** `ed5bdab293e3bdfad119a9eeb0b30201d5f9874fbe9466cf16e3aebcdac1e62d`
- **label_dataset:** `docs/audit/ml-label-dataset-v8.json`
- **label_dataset_sha256:** `ea65b114cfef6e22e3299bf4e54d320b6ba13a66185ac0908700528a1846948c`
- **joined rows:** `427`
- **random_seed:** `0`

## Slice Definitions

| Slice | Definition |
|---|---|
| `external_near_miss` | review_pool_variant == "ml_external_near_miss_audit" |
| `blind_snapshot` | review_pool_variant == "ml_blind_snapshot_audit" |
| `rank_shaped_family` | review_pool_variant in {full_family_top_k, bridge_eligible_only, ml_contrastive_offline_audit, ml_emerging_target_gap_audit:good_or_acceptable} |
| `hard_negative` | review_pool_variant == "ml_hard_negative_audit" |
| `transfer_gap` | review_pool_variant == "ml_transfer_gap_audit" |
| `legacy_or_uncategorized` | review_pool_variant is null, empty, or whitespace-only |

## good_or_acceptable

- **eligible rows:** `427`
- **excluded rows:** `0`

### In-Pool CV Summary

| Slice | Model | Skipped | Balanced accuracy | Macro F1 | ROC-AUC | TN | FP | FN | TP |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `external_near_miss` | `embedding_logistic` | false | 0.771 | 0.795 | 0.944 | 41 | 2 | 7 | 10 |
| `external_near_miss` | `majority_train_baseline` | false | 0.500 | 0.417 | null | 43 | 0 | 17 | 0 |
| `external_near_miss` | `train_prevalence_score_baseline` | false | 0.500 | 0.417 | null | 43 | 0 | 17 | 0 |
| `external_near_miss` | `metadata_sample_reason_logistic` | false | 0.477 | 0.406 | 0.617 | 41 | 2 | 17 | 0 |
| `blind_snapshot` | `embedding_logistic` | false | 0.495 | 0.476 | 0.670 | 0 | 10 | 1 | 109 |
| `blind_snapshot` | `majority_train_baseline` | false | 0.500 | 0.478 | null | 0 | 10 | 0 | 110 |
| `blind_snapshot` | `train_prevalence_score_baseline` | false | 0.500 | 0.478 | null | 0 | 10 | 0 | 110 |
| `blind_snapshot` | `metadata_sample_reason_logistic` | false | 0.500 | 0.478 | 0.270 | 0 | 10 | 0 | 110 |
| `rank_shaped_family` | `embedding_logistic` | false | 0.796 | 0.827 | 0.962 | 13 | 8 | 3 | 106 |
| `rank_shaped_family` | `majority_train_baseline` | false | 0.500 | 0.456 | null | 0 | 21 | 0 | 109 |
| `rank_shaped_family` | `train_prevalence_score_baseline` | false | 0.500 | 0.456 | null | 0 | 21 | 0 | 109 |
| `rank_shaped_family` | `metadata_sample_reason_logistic` | false | 0.500 | 0.456 | 0.477 | 0 | 21 | 0 | 109 |
| `hard_negative` | all | true: slice lacks enough rows in both classes for stratified CV |  |  |  |  |  |  |  |
| `transfer_gap` | all | true: slice lacks enough rows in both classes for stratified CV |  |  |  |  |  |  |  |
| `legacy_or_uncategorized` | all | true: slice lacks enough rows in both classes for stratified CV |  |  |  |  |  |  |  |

### Transfer Summary

| Comparison | Model | Skipped | Balanced accuracy | Macro F1 | ROC-AUC | TN | FP | FN | TP |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `external_near_miss_to_blind_snapshot` | `embedding_logistic` | false | 0.691 | 0.390 | 0.764 | 10 | 0 | 68 | 42 |
| `external_near_miss_to_blind_snapshot` | `majority_train_baseline` | false | 0.500 | 0.077 | null | 10 | 0 | 110 | 0 |
| `external_near_miss_to_blind_snapshot` | `train_prevalence_score_baseline` | false | 0.500 | 0.077 | null | 10 | 0 | 110 | 0 |
| `blind_snapshot_to_external_near_miss` | `embedding_logistic` | false | 0.558 | 0.340 | 0.767 | 5 | 38 | 0 | 17 |
| `blind_snapshot_to_external_near_miss` | `majority_train_baseline` | false | 0.500 | 0.221 | null | 0 | 43 | 0 | 17 |
| `blind_snapshot_to_external_near_miss` | `train_prevalence_score_baseline` | false | 0.500 | 0.221 | null | 0 | 43 | 0 | 17 |
| `rank_shaped_family_to_external_near_miss` | `embedding_logistic` | false | 0.556 | 0.506 | 0.642 | 20 | 23 | 6 | 11 |
| `rank_shaped_family_to_external_near_miss` | `majority_train_baseline` | false | 0.500 | 0.221 | null | 0 | 43 | 0 | 17 |
| `rank_shaped_family_to_external_near_miss` | `train_prevalence_score_baseline` | false | 0.500 | 0.221 | null | 0 | 43 | 0 | 17 |
| `rank_shaped_family_to_blind_snapshot` | `embedding_logistic` | false | 0.577 | 0.588 | 0.653 | 2 | 8 | 5 | 105 |
| `rank_shaped_family_to_blind_snapshot` | `majority_train_baseline` | false | 0.500 | 0.478 | null | 0 | 10 | 0 | 110 |
| `rank_shaped_family_to_blind_snapshot` | `train_prevalence_score_baseline` | false | 0.500 | 0.478 | null | 0 | 10 | 0 | 110 |
| `external_near_miss_plus_blind_snapshot_to_rank_shaped_family` | `embedding_logistic` | false | 0.634 | 0.670 | 0.761 | 6 | 15 | 2 | 107 |
| `external_near_miss_plus_blind_snapshot_to_rank_shaped_family` | `majority_train_baseline` | false | 0.500 | 0.456 | null | 0 | 21 | 0 | 109 |
| `external_near_miss_plus_blind_snapshot_to_rank_shaped_family` | `train_prevalence_score_baseline` | false | 0.500 | 0.456 | null | 0 | 21 | 0 | 109 |
| `all_not_external_near_miss_to_external_near_miss` | `embedding_logistic` | false | 0.772 | 0.762 | 0.865 | 36 | 7 | 5 | 12 |
| `all_not_external_near_miss_to_external_near_miss` | `majority_train_baseline` | false | 0.500 | 0.221 | null | 0 | 43 | 0 | 17 |
| `all_not_external_near_miss_to_external_near_miss` | `train_prevalence_score_baseline` | false | 0.500 | 0.221 | null | 0 | 43 | 0 | 17 |
| `all_not_blind_snapshot_to_blind_snapshot` | `embedding_logistic` | false | 0.700 | 0.594 | 0.814 | 6 | 4 | 22 | 88 |
| `all_not_blind_snapshot_to_blind_snapshot` | `majority_train_baseline` | false | 0.500 | 0.478 | null | 0 | 10 | 0 | 110 |
| `all_not_blind_snapshot_to_blind_snapshot` | `train_prevalence_score_baseline` | false | 0.500 | 0.478 | null | 0 | 10 | 0 | 110 |

## surprising_or_useful

- **eligible rows:** `427`
- **excluded rows:** `0`

### In-Pool CV Summary

| Slice | Model | Skipped | Balanced accuracy | Macro F1 | ROC-AUC | TN | FP | FN | TP |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `external_near_miss` | `embedding_logistic` | false | 0.752 | 0.759 | 0.840 | 14 | 8 | 5 | 33 |
| `external_near_miss` | `majority_train_baseline` | false | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| `external_near_miss` | `train_prevalence_score_baseline` | false | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| `external_near_miss` | `metadata_sample_reason_logistic` | false | 0.678 | 0.687 | 0.725 | 9 | 13 | 2 | 36 |
| `blind_snapshot` | `embedding_logistic` | false | 0.538 | 0.537 | 0.761 | 2 | 17 | 3 | 98 |
| `blind_snapshot` | `majority_train_baseline` | false | 0.500 | 0.457 | null | 0 | 19 | 0 | 101 |
| `blind_snapshot` | `train_prevalence_score_baseline` | false | 0.500 | 0.457 | null | 0 | 19 | 0 | 101 |
| `blind_snapshot` | `metadata_sample_reason_logistic` | false | 0.500 | 0.457 | 0.435 | 0 | 19 | 0 | 101 |
| `rank_shaped_family` | `embedding_logistic` | false | 0.716 | 0.736 | 0.869 | 14 | 14 | 7 | 95 |
| `rank_shaped_family` | `majority_train_baseline` | false | 0.500 | 0.440 | null | 0 | 28 | 0 | 102 |
| `rank_shaped_family` | `train_prevalence_score_baseline` | false | 0.500 | 0.440 | null | 0 | 28 | 0 | 102 |
| `rank_shaped_family` | `metadata_sample_reason_logistic` | false | 0.500 | 0.440 | 0.473 | 0 | 28 | 0 | 102 |
| `hard_negative` | all | true: slice lacks enough rows in both classes for stratified CV |  |  |  |  |  |  |  |
| `transfer_gap` | `embedding_logistic` | false | 0.737 | 0.751 | 0.848 | 28 | 3 | 6 | 8 |
| `transfer_gap` | `majority_train_baseline` | false | 0.500 | 0.408 | null | 31 | 0 | 14 | 0 |
| `transfer_gap` | `train_prevalence_score_baseline` | false | 0.500 | 0.408 | null | 31 | 0 | 14 | 0 |
| `transfer_gap` | `metadata_sample_reason_logistic` | false | 0.500 | 0.408 | 0.517 | 31 | 0 | 14 | 0 |
| `legacy_or_uncategorized` | all | true: slice lacks enough rows in both classes for stratified CV |  |  |  |  |  |  |  |

### Transfer Summary

| Comparison | Model | Skipped | Balanced accuracy | Macro F1 | ROC-AUC | TN | FP | FN | TP |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `external_near_miss_to_blind_snapshot` | `embedding_logistic` | false | 0.490 | 0.452 | 0.431 | 0 | 19 | 2 | 99 |
| `external_near_miss_to_blind_snapshot` | `majority_train_baseline` | false | 0.500 | 0.457 | null | 0 | 19 | 0 | 101 |
| `external_near_miss_to_blind_snapshot` | `train_prevalence_score_baseline` | false | 0.500 | 0.457 | null | 0 | 19 | 0 | 101 |
| `blind_snapshot_to_external_near_miss` | `embedding_logistic` | false | 0.506 | 0.450 | 0.396 | 2 | 20 | 3 | 35 |
| `blind_snapshot_to_external_near_miss` | `majority_train_baseline` | false | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| `blind_snapshot_to_external_near_miss` | `train_prevalence_score_baseline` | false | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| `rank_shaped_family_to_external_near_miss` | `embedding_logistic` | false | 0.755 | 0.741 | 0.813 | 17 | 5 | 10 | 28 |
| `rank_shaped_family_to_external_near_miss` | `majority_train_baseline` | false | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| `rank_shaped_family_to_external_near_miss` | `train_prevalence_score_baseline` | false | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| `rank_shaped_family_to_blind_snapshot` | `embedding_logistic` | false | 0.510 | 0.510 | 0.488 | 3 | 16 | 14 | 87 |
| `rank_shaped_family_to_blind_snapshot` | `majority_train_baseline` | false | 0.500 | 0.457 | null | 0 | 19 | 0 | 101 |
| `rank_shaped_family_to_blind_snapshot` | `train_prevalence_score_baseline` | false | 0.500 | 0.457 | null | 0 | 19 | 0 | 101 |
| `external_near_miss_plus_blind_snapshot_to_rank_shaped_family` | `embedding_logistic` | false | 0.651 | 0.683 | 0.703 | 9 | 19 | 2 | 100 |
| `external_near_miss_plus_blind_snapshot_to_rank_shaped_family` | `majority_train_baseline` | false | 0.500 | 0.440 | null | 0 | 28 | 0 | 102 |
| `external_near_miss_plus_blind_snapshot_to_rank_shaped_family` | `train_prevalence_score_baseline` | false | 0.500 | 0.440 | null | 0 | 28 | 0 | 102 |
| `all_not_external_near_miss_to_external_near_miss` | `embedding_logistic` | false | 0.689 | 0.663 | 0.787 | 17 | 5 | 15 | 23 |
| `all_not_external_near_miss_to_external_near_miss` | `majority_train_baseline` | false | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| `all_not_external_near_miss_to_external_near_miss` | `train_prevalence_score_baseline` | false | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| `all_not_blind_snapshot_to_blind_snapshot` | `embedding_logistic` | false | 0.482 | 0.471 | 0.500 | 1 | 18 | 9 | 92 |
| `all_not_blind_snapshot_to_blind_snapshot` | `majority_train_baseline` | false | 0.500 | 0.457 | null | 0 | 19 | 0 | 101 |
| `all_not_blind_snapshot_to_blind_snapshot` | `train_prevalence_score_baseline` | false | 0.500 | 0.457 | null | 0 | 19 | 0 | 101 |

## What This Means

This artifact compares in-source and cross-source text-only signal. Differences can reflect real label signal, source/worksheet selection effects, label imbalance, and text-format confounding; interpret as diagnostic evidence, not causal proof.

## Not A Production Recommender Test

This is not a production recommender test. Production-grade evaluation would still require deliberate splits, larger and multi-reviewer labels, product-matched candidate pools, top-k workflow metrics, and shadow or flagged experiments.

## Caveats

- Not validation; single-reviewer audit labels; observation-level duplicates/conflicts preserved.
- Source-transfer diagnostic only; not production ranking evidence.
- Text format differs across rows (verbatim external vs OpenAlex-hydrated labeled format) and can be confounded with review_pool_variant; interpret cautiously, not causally.
- No persistent train/dev/test split artifact.
