# Text Transfer Readiness (ml-text-transfer-readiness-v8)

## Executive Summary

- Offline synthesis only: no new embeddings, model training, ranking, Postgres, or label mutation.
- Explicit labeled audit rows: `427`.
- Duplicate paper pressure: `85` duplicate observations beyond first paper IDs.
- In-pool text signal exists for at least some slices, but cross-source transfer is uneven.
- `surprising_or_useful` remains the least stable transfer target and needs rubric/label work before trust.
- Text-format evidence: Text-format normalization did not change text_for_embedding values in ml-labeled-text-corpus-v3-normalized; regenerating embeddings solely for text formatting is unnecessary for this dataset. The observed transfer differences are unlikely to be explained by title/abstract string packaging alone, though source selection and label context remain confounds.
- Production readiness remains explicitly false.

## Class Balance

| target | review_pool_variant | true | false | null | total |
| --- | --- | ---: | ---: | ---: | ---: |
| `good_or_acceptable` | `(null)` | 65 | 0 | 0 | 65 |
| `good_or_acceptable` | `bridge_eligible_only` | 19 | 1 | 0 | 20 |
| `good_or_acceptable` | `full_family_top_k` | 39 | 1 | 0 | 40 |
| `good_or_acceptable` | `ml_blind_snapshot_audit` | 110 | 10 | 0 | 120 |
| `good_or_acceptable` | `ml_contrastive_offline_audit` | 41 | 4 | 0 | 45 |
| `good_or_acceptable` | `ml_emerging_target_gap_audit:good_or_acceptable` | 10 | 15 | 0 | 25 |
| `good_or_acceptable` | `ml_external_near_miss_audit` | 17 | 43 | 0 | 60 |
| `good_or_acceptable` | `ml_hard_negative_audit` | 7 | 0 | 0 | 7 |
| `good_or_acceptable` | `ml_transfer_gap_audit` | 1 | 44 | 0 | 45 |
| `surprising_or_useful` | `(null)` | 65 | 0 | 0 | 65 |
| `surprising_or_useful` | `bridge_eligible_only` | 18 | 2 | 0 | 20 |
| `surprising_or_useful` | `full_family_top_k` | 36 | 4 | 0 | 40 |
| `surprising_or_useful` | `ml_blind_snapshot_audit` | 101 | 19 | 0 | 120 |
| `surprising_or_useful` | `ml_contrastive_offline_audit` | 38 | 7 | 0 | 45 |
| `surprising_or_useful` | `ml_emerging_target_gap_audit:good_or_acceptable` | 10 | 15 | 0 | 25 |
| `surprising_or_useful` | `ml_external_near_miss_audit` | 38 | 22 | 0 | 60 |
| `surprising_or_useful` | `ml_hard_negative_audit` | 6 | 1 | 0 | 7 |
| `surprising_or_useful` | `ml_transfer_gap_audit` | 14 | 31 | 0 | 45 |

## In-Pool Signal Summary

| target | slice | skipped | balanced_accuracy | roc_auc | macro_f1 |
| --- | --- | --- | ---: | ---: | ---: |
| `good_or_acceptable` | `external_near_miss` | `False` | 0.771 | 0.944 | 0.795 |
| `good_or_acceptable` | `blind_snapshot` | `False` | 0.495 | 0.670 | 0.476 |
| `good_or_acceptable` | `rank_shaped_family` | `False` | 0.796 | 0.962 | 0.827 |
| `good_or_acceptable` | `hard_negative` | `True` | n/a | n/a | n/a |
| `good_or_acceptable` | `transfer_gap` | `True` | n/a | n/a | n/a |
| `good_or_acceptable` | `legacy_or_uncategorized` | `True` | n/a | n/a | n/a |
| `surprising_or_useful` | `external_near_miss` | `False` | 0.752 | 0.840 | 0.759 |
| `surprising_or_useful` | `blind_snapshot` | `False` | 0.538 | 0.761 | 0.537 |
| `surprising_or_useful` | `rank_shaped_family` | `False` | 0.716 | 0.869 | 0.736 |
| `surprising_or_useful` | `hard_negative` | `True` | n/a | n/a | n/a |
| `surprising_or_useful` | `transfer_gap` | `False` | 0.737 | 0.848 | 0.751 |
| `surprising_or_useful` | `legacy_or_uncategorized` | `True` | n/a | n/a | n/a |

## Transfer Summary

| target | comparison | skipped | balanced_accuracy | roc_auc | macro_f1 |
| --- | --- | --- | ---: | ---: | ---: |
| `good_or_acceptable` | `external_near_miss_to_blind_snapshot` | `False` | 0.691 | 0.764 | 0.390 |
| `good_or_acceptable` | `blind_snapshot_to_external_near_miss` | `False` | 0.558 | 0.767 | 0.340 |
| `good_or_acceptable` | `rank_shaped_family_to_external_near_miss` | `False` | 0.556 | 0.642 | 0.506 |
| `good_or_acceptable` | `rank_shaped_family_to_blind_snapshot` | `False` | 0.577 | 0.653 | 0.588 |
| `good_or_acceptable` | `external_near_miss_plus_blind_snapshot_to_rank_shaped_family` | `False` | 0.634 | 0.761 | 0.670 |
| `good_or_acceptable` | `all_not_external_near_miss_to_external_near_miss` | `False` | 0.772 | 0.865 | 0.762 |
| `good_or_acceptable` | `all_not_blind_snapshot_to_blind_snapshot` | `False` | 0.700 | 0.814 | 0.594 |
| `surprising_or_useful` | `external_near_miss_to_blind_snapshot` | `False` | 0.490 | 0.431 | 0.452 |
| `surprising_or_useful` | `blind_snapshot_to_external_near_miss` | `False` | 0.506 | 0.396 | 0.450 |
| `surprising_or_useful` | `rank_shaped_family_to_external_near_miss` | `False` | 0.755 | 0.813 | 0.741 |
| `surprising_or_useful` | `rank_shaped_family_to_blind_snapshot` | `False` | 0.510 | 0.488 | 0.510 |
| `surprising_or_useful` | `external_near_miss_plus_blind_snapshot_to_rank_shaped_family` | `False` | 0.651 | 0.703 | 0.683 |
| `surprising_or_useful` | `all_not_external_near_miss_to_external_near_miss` | `False` | 0.689 | 0.787 | 0.663 |
| `surprising_or_useful` | `all_not_blind_snapshot_to_blind_snapshot` | `False` | 0.482 | 0.500 | 0.471 |

## Text-Format Evidence

- `n_text_changed_from_v1`: `0`
- conclusion: Text-format normalization did not change text_for_embedding values in ml-labeled-text-corpus-v3-normalized; regenerating embeddings solely for text formatting is unnecessary for this dataset. The observed transfer differences are unlikely to be explained by title/abstract string packaging alone, though source selection and label context remain confounds.

## Decisions / Next Steps

- P1: Clarify the surprising_or_useful rubric and collect more balanced labels across external, blind, and rank-shaped sources. Rationale: This target shows stronger source sensitivity and weaker external/blind transfer in the current diagnostic.
- P2: Expand cross-source labeling for good_or_acceptable and use future offline ranker experiments only as research probes. Rationale: The target shows learnable in-pool text signal, but source-transfer evidence is not a production gate.
- P3: Keep production recommender changes blocked until the missing gates are explicitly addressed. Rationale: The current artifacts are audit diagnostics, not live recommender validation.

## Heuristic Flags

- `good_or_acceptable`: in_pool_signal_strong=True, external_blind_transfer_weak=False, transfer_inconsistent=False, needs_more_labels=True, production_ready=False
- `surprising_or_useful`: in_pool_signal_strong=True, external_blind_transfer_weak=True, transfer_inconsistent=True, needs_more_labels=True, production_ready=False

## Not Doing Yet

- multi-reviewer or adjudication policy
- deliberate split policy
- product-matched candidate pools
- top-k workflow metrics
- leakage controls
- shadow/flagged experiment plan

## Caveats

- Not validation.
- Single-reviewer audit labels.
- Heuristic synthesis only.
- No production ranking implication.
- Observation-level duplicates/conflicts are preserved.
- Source selection, label context, class imbalance, and text source remain possible confounds.
- No new model training, embeddings, ranking, or splits were created.
