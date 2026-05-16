# Offline Ranker Experiment (ml-offline-ranker-experiment-v1)

## Eligibility Summary

- **Target:** `good_or_acceptable`
- **Eligible observations:** 427
- **Unique canonical works:** 342
- **Duplicate observation pressure:** 85
- **Effective grouped CV folds:** 5

## Class Balance

| Level | Positive | Negative |
| --- | ---: | ---: |
| Observation | 309 | 118 |
| Work any-positive | 228 | 114 |

## Model Summary

| Model | Balanced Accuracy Mean | ROC-AUC Mean | Average Precision Mean | Macro F1 Mean | TN | FP | FN | TP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `majority_class` | 0.500 | n/a | n/a | 0.420 | 0 | 118 | 0 | 309 |
| `prevalence_probability` | 0.500 | 0.500 | 0.724 | 0.420 | 0 | 118 | 0 | 309 |
| `embedding_logistic` | 0.854 | 0.939 | 0.975 | 0.864 | 90 | 28 | 17 | 292 |

## Leakage

- **Global work-overlap count:** 0
- **Leakage checks passed:** True

## Caveats

- Not validation.
- Offline diagnostic only.
- Single-reviewer audit labels and rubric limits remain.
- Observation-level labels are preserved; duplicate/conflicting work observations are not silently merged.
- Grouped CV reduces same-work leakage but does not eliminate source-selection, pool, or label-context bias.
- No production ranking/API/web behavior change is supported.
- No production model artifact is produced.

## Not Production

This artifact is not validation, not shadow scoring, and not production ranking evidence. No model file, API behavior, web behavior, or ranking default is changed.

## Next Step

If metrics are credible after review, define `ml-offline-metric-gates-v1`; otherwise prioritize labeling and rubric work. No shadow scoring should start without metric gates.
