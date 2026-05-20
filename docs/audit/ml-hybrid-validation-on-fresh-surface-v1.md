# Hybrid Validation On Fresh Surface v1

## Executive Summary

Frozen holdout scorer v2 and pre-registered hybrid arms were applied to the ready fresh hybrid eval surface. This artifact produces metrics for a later gates command; it does not pass confirmatory validation or authorize shadow/production.

- **candidate pool:** 358
- **confirmatory metric works:** 143
- **label balance:** 54 positive / 89 negative
- **embedding coverage:** 358 / 358 (fresh-hybrid-text-embedding-v1)
- **primary confirmatory arm:** `hybrid_rank_mean_50_50`
- **primary arm material lift vs heuristic:** True
- **recommended next stage:** `run_hybrid_validation_metric_gates_v1`

## Candidate And Label Coverage

| Item | Value |
| --- | ---: |
| Candidate pool works | 358 |
| Confirmatory metric works | 143 |
| Positive works | 54 |
| Negative works | 89 |
| Positive prevalence | 0.3776 |

## Arm Metrics

| Arm | ROC-AUC | AP | P@5 | P@10 | P@20 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `heuristic_final_score_baseline` | 0.8252 | 0.6494 | 0.6000 | 0.5000 | 0.5000 |
| `holdout_embedding_probability_baseline` | 0.8502 | 0.7513 | 0.6000 | 0.7000 | 0.8500 |
| `hybrid_rank_mean_50_50` | 0.9103 | 0.8617 | 1.0000 | 1.0000 | 0.9500 |
| `hybrid_rank_mean_75_25_heuristic` | 0.8835 | 0.7980 | 1.0000 | 1.0000 | 0.8500 |
| `hybrid_rank_mean_25_75_heuristic` | 0.8851 | 0.8208 | 1.0000 | 0.9000 | 0.8500 |

## Deltas Vs Heuristic

| Arm | delta ROC-AUC | delta AP | delta P@5 | delta P@10 | delta P@20 | Material lift |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `holdout_embedding_probability_baseline` | 0.0250 | 0.1019 | 0.0000 | 0.2000 | 0.3500 | True |
| `hybrid_rank_mean_50_50` | 0.0851 | 0.2123 | 0.4000 | 0.5000 | 0.4500 | True |
| `hybrid_rank_mean_75_25_heuristic` | 0.0583 | 0.1486 | 0.4000 | 0.5000 | 0.3500 | True |
| `hybrid_rank_mean_25_75_heuristic` | 0.0599 | 0.1714 | 0.4000 | 0.4000 | 0.3500 | True |

## Exploratory Best Arms

- **Best by ROC-AUC:** `hybrid_rank_mean_50_50` (0.9103)
- **Best by AP:** `hybrid_rank_mean_50_50` (0.8617)
- Best-arm selection is exploratory only; the primary confirmatory arm remains fixed at `hybrid_rank_mean_50_50`.

## Leakage Checks

- Old 217 overlap is excluded from confirmatory metrics.
- Confirmatory rows with previous-eval overlap: 0.
- Supervised fit, eval-label weight tuning, scorer refit: false.

## Not Shadow / Not Production

- `shadow_scoring_allowed`: false
- `production_default_allowed`: false
- `confirmatory_validation_passed`: false until a separate gates command runs.

## Caveats

- Not live recommender validation.
- Fresh confirmatory surface; old 217 overlaps are excluded from confirmatory metrics.
- Frozen holdout scorer v2 applied without refit.
- Single-reviewer audit labels.
- Best-arm metrics are exploratory only; primary confirmatory arm is fixed at hybrid_rank_mean_50_50.
- No shadow/production authorization.
- confirmatory_validation_passed requires a separate metric gates command.
