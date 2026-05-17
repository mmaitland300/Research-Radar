# Hybrid Scorer Offline Experiment (ml-hybrid-scorer-offline-experiment-v1)

## Executive Summary

This executes the pre-registered hybrid scorer offline experiment on scoring v3 JSON only. It uses fixed label-blind arms, no fitting, no DB, and no ranking writes.

- **Hybrid material lift passed:** True
- **Recommended next stage:** `create_hybrid_scorer_metric_gates_v1`
- **Best ROC-AUC arm:** `hybrid_rank_mean_50_50`
- **Best AP arm:** `hybrid_rank_mean_50_50`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Candidate/Eval Coverage

| Measure | Value |
| --- | ---: |
| Candidate pool works | 217 |
| Assignment eval works | 217 |
| Labeled eval metric works | 217 |
| Positive eval works | 190 |
| Negative eval works | 27 |
| Positive work prevalence | 0.876 |

## Arm Metrics

| Arm | ROC-AUC | AP | P@5 | P@10 | P@20 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `heuristic_final_score_baseline` | 0.804 | 0.958 | 1.000 | 1.000 | 1.000 |
| `holdout_embedding_probability_baseline` | 0.805 | 0.967 | 1.000 | 1.000 | 1.000 |
| `hybrid_rank_mean_50_50` | 0.846 | 0.974 | 1.000 | 1.000 | 1.000 |
| `hybrid_rank_mean_75_25_heuristic` | 0.827 | 0.966 | 1.000 | 1.000 | 1.000 |
| `hybrid_rank_mean_25_75_heuristic` | 0.836 | 0.973 | 1.000 | 1.000 | 1.000 |

## Deltas Vs Heuristic

| Arm | delta ROC-AUC | delta AP | delta P@5 | delta P@10 | delta P@20 | Material lift |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `holdout_embedding_probability_baseline` | 0.001 | 0.009 | 0.000 | 0.000 | 0.000 | False |
| `hybrid_rank_mean_50_50` | 0.042 | 0.016 | 0.000 | 0.000 | 0.000 | True |
| `hybrid_rank_mean_75_25_heuristic` | 0.024 | 0.009 | 0.000 | 0.000 | 0.000 | False |
| `hybrid_rank_mean_25_75_heuristic` | 0.032 | 0.015 | 0.000 | 0.000 | 0.000 | True |

## Best-Arm Exploratory Warning

Best-arm selection is exploratory only: True. This experiment evaluates an already-seen v3 eval surface.

## Material Lift Assessment

- Hybrid material lift passed: True
- Material lift requires a hybrid arm to beat heuristic by ROC-AUC >= 0.03 or AP >= 0.02.

## Prevalence/P@k Caveat

The eval set is positive-heavy, so P@k is advisory when arms are saturated.

## Leakage Checks

- Train rows used in metrics: 0
- Train works used in metrics: 0
- Supervised fit used: False
- Eval-label weight tuning used: False

## Recommended Next Stage

`create_hybrid_scorer_metric_gates_v1`

## Not Shadow / Not Production

- Shadow scoring allowed: False
- Production default allowed: False
- Missing hybrid metric gates: True
- Missing `ml-shadow-scorer-v1`: True
- No production model artifact: True

## Caveats

- Not live recommender validation.
- Pre-registered arms are evaluated on already-seen v3 eval surface.
- Best-arm selection is exploratory only.
- Single reviewer.
- One ranking run/family.
- Positive-heavy P@k.
- No shadow/production authorization.
