# Bridge rank-percentile hybrid controlled rollout replay v1

Offline controlled rollout replay for replacing the current Bridge top-20 with the rank-percentile hybrid top-20. No serving, API, web, or database-write behavior changed.

## Prerequisite

- Sensitivity artifact: `docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json`
- SHA256: `04a41f91cee1a2a78b7f1f8e9f99b1ef13679a8dacd1b404a848c82751d807d2`
- Selected C: `0.001`

## Arm Comparison

| arm | labeled | pos | neg | unlabeled | labeled precision |
|---|---:|---:|---:|---:|---:|
| `current_bridge` | 20 | 8 | 12 | 0 | 0.4 |
| `pure_ml` | 11 | 11 | 0 | 9 | 1.0 |
| `pure_bridge` | 9 | 4 | 5 | 11 | 0.4444444444444444 |
| `hybrid_alpha_0_5` | 13 | 13 | 0 | 7 | 1.0 |
| `hybrid_alpha_0_7` | 12 | 12 | 0 | 8 | 1.0 |

## Top-20 Churn

| alpha | stable | promoted | demoted | churn fraction | proposed labeled precision |
|---:|---:|---:|---:|---:|---:|
| 0.5 | 0 | 20 | 20 | 1.0 | 1.0 |
| 0.7 | 1 | 19 | 19 | 0.95 | 1.0 |

## Verdict Table

| readout | count / value |
|---|---:|
| promoted_labeled_negatives | 0 |
| promoted_unlabeled_high_risk | 0 |
| demoted_labeled_positive_clear_loss | 0 |
| stable_labeled_positives | 0 |
| current labeled precision | 0.4 |
| proposed labeled precision | 1.0 |
| top20_quality_delta_labeled_only | 0.6 |

## Recommendation

- **recommended_next_stage:** `draft_bridge_rank_pct_hybrid_serving_plan_v1`
- Primary alpha=0.5 introduces no promoted labeled negatives or high-risk unlabeled promotions, does not reduce labeled top-20 precision, and has no clear-loss demoted positives.

## Promoted

- `W4417471638` current=505 hybrid=1 label=True
- `W7126213550` current=377 hybrid=2 label=True
- `W7128741623` current=385 hybrid=3 label=True
- `W4415337516` current=186 hybrid=4 label=True
- `W4401445948` current=183 hybrid=5 label=True
- `W4413133017` current=453 hybrid=6 label=True
- `W7155391647` current=242 hybrid=7 label=None
- `W4411141659` current=54 hybrid=8 label=True
- `W7150806375` current=279 hybrid=9 label=None
- `W4404570638` current=311 hybrid=10 label=True
- `W4414818544` current=321 hybrid=11 label=True
- `W7131744631` current=292 hybrid=12 label=None
- `W4414011891` current=32 hybrid=13 label=True
- `W4414112292` current=477 hybrid=14 label=None
- `W7116661261` current=511 hybrid=15 label=None
- `W4409474100` current=91 hybrid=16 label=True
- `W4410025358` current=33 hybrid=17 label=True
- `W4401727628` current=61 hybrid=18 label=True
- `W4412072230` current=65 hybrid=19 label=None
- `W4414581097` current=315 hybrid=20 label=None

## Demoted

- `W4411141874` current=1 hybrid=128 label=True
- `W4408772031` current=2 hybrid=517 label=False
- `W4409215217` current=3 hybrid=97 label=True
- `W4409215261` current=4 hybrid=413 label=False
- `W4410091503` current=5 hybrid=494 label=False
- `W4409967775` current=6 hybrid=500 label=False
- `W7131681001` current=7 hybrid=110 label=True
- `W4415947443` current=8 hybrid=339 label=True
- `W4409474070` current=9 hybrid=395 label=False
- `W4411649085` current=10 hybrid=29 label=True
- `W4402645135` current=11 hybrid=506 label=False
- `W4412900768` current=12 hybrid=439 label=False
- `W4411141958` current=13 hybrid=55 label=True
- `W4409215215` current=14 hybrid=23 label=True
- `W7128595024` current=15 hybrid=436 label=False
- `W4414799687` current=16 hybrid=474 label=False
- `W4407236737` current=17 hybrid=435 label=False
- `W7116976483` current=18 hybrid=441 label=False
- `W4414199528` current=19 hybrid=243 label=False
- `W4416267785` current=20 hybrid=146 label=True

## Caveats

- Offline controlled rollout replay only.
- Does not enable Bridge serving.
- Uses frozen v3 C=0.001 scorer.
- Full-pool ML probabilities are frozen-model inference, not OOF estimates.
- Top-20 labeled precision is underpowered; most pool papers are unlabeled.
- Label overlay is incomplete; unlabeled promoted papers may need review.
- No API/web/production behavior changed.
