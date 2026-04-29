# Bridge objective redesign simulation

> Diagnostic only; not validation.
> No ranking/default/product claim.
> No DB writes; read-only `SELECT` against `paper_scores` / `works` only.
> No new eligibility rule is selected from this artifact alone.
> Any candidate requires a follow-up zero-weight ranking run and labels before policy change.

- **ranking_run_id:** `rank-ee2ba6c816`
- **reference Jaccard (existing eligible vs emerging top-20):** `0.212121`

## Variants

| variant_id | type | eligible_total | top20 | Jaccard | delta | new_unlabeled | candidate | too_blunt | cohort_risk |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| `existing_bridge_eligible_baseline` | baseline | 93 | 20 | 0.212121 | 0.0 | 0 | False | False | False |
| `eligible_exclude_emerging_top_20` | hard_exclusion | 83 | 20 | 0.0 | -0.212121 | 7 | True | False | False |
| `eligible_exclude_emerging_top_50` | hard_exclusion | 69 | 20 | 0.0 | -0.212121 | 17 | False | False | False |
| `eligible_exclude_persistent_shared_with_emerging` | hard_exclusion | 88 | 20 | 0.081081 | -0.13104 | 5 | True | False | False |
| `residual_bridge_score_minus_0.1_emerging_final_score` | residual_penalty | 93 | 20 | 0.025641 | -0.18648 | 19 | False | False | False |
| `residual_bridge_score_minus_0.25_emerging_final_score` | residual_penalty | 93 | 20 | 0.0 | -0.212121 | 20 | False | False | False |
| `residual_bridge_score_minus_0.5_emerging_final_score` | residual_penalty | 93 | 20 | 0.0 | -0.212121 | 20 | False | False | False |
| `residual_bridge_score_minus_1_emerging_final_score` | residual_penalty | 93 | 20 | 0.0 | -0.212121 | 20 | False | False | False |
| `eligible_and_residual_lambda_0_25` | combined | 93 | 20 | 0.0 | -0.212121 | 20 | False | False | False |
| `eligible_exclude_emerging_top_50_and_residual_lambda_0_25` | combined | 69 | 20 | 0.0 | -0.212121 | 20 | False | False | False |

## Summary

- **best_hard_exclusion_variant_id (lowest Jaccard, tie-break fewer new unlabeled):** `eligible_exclude_emerging_top_20`
- **best_hard_exclusion_candidate_lowest_new_unlabeled_variant_id:** `eligible_exclude_persistent_shared_with_emerging`
- **best_residual_penalty_variant_id:** `residual_bridge_score_minus_0.25_emerging_final_score`
- **any_variant_beats_jaccard_floor_with_full_top20_and_cohort_ge_50:** `True`
- **candidate_variant_ids:** `['eligible_exclude_emerging_top_20', 'eligible_exclude_persistent_shared_with_emerging']`

## Recommended next step

At most: plan a follow-up **zero-weight** ranking experiment if a candidate passes the gate; do not treat simulation output as a production rule or default bridge arm.
