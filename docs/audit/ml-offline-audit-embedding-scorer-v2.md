# Offline Audit Embedding Scorer Export (ml-offline-audit-embedding-scorer-v2)

## Executive Summary

Frozen JSON scorer fit only on the holdout assignment train arm for `good_or_acceptable`. Product-candidate eval works are reserved for scoring v3.

- **Target:** `good_or_acceptable`
- **Fit mode:** `holdout_bound_train_only`
- **Train observations:** 141
- **Train works:** 125
- **Eval works excluded:** 217
- **Eval work-set SHA:** `213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a`
- **Shadow scoring authorized:** False
- **Production artifact written:** False

## Leakage Checks

- **Holdout assignment honored:** True
- **Eval works excluded from fit:** True
- **Train/eval work overlap count:** 0

## Train Class Balance

| Level | Count | Positive | Negative | Conflicts |
| --- | ---: | ---: | ---: | ---: |
| Observation | 141 | 54 | 87 | n/a |
| Work any-positive | 125 | 38 | 87 | 0 |

## Scorer Summary

- **Dimensions:** 1536
- **Classes:** `[False, True]`
- **Random seed:** 20260515
- **Pipeline:** `StandardScaler(with_mean=True) -> LogisticRegression(solver='lbfgs', penalty='l2', max_iter=5000)`

## In-Sample Train Metrics

**IN_SAMPLE_TRAIN_ARM_ONLY_NOT_VALIDATION - NOT VALIDATION**

| Metric | Value |
| --- | ---: |
| Accuracy | 1.000 |
| Balanced accuracy | 1.000 |
| Macro F1 | 1.000 |
| ROC-AUC | 1.000 |
| Average precision | 1.000 |
| TN | 87 |
| FP | 0 |
| FN | 0 |
| TP | 54 |

## Baselines

- **Grouped CV baseline:** referenced from `docs/audit/ml-offline-ranker-experiment-v1.json`; fold coefficients not reused.
- **V1 full-fit reference:** ROC-AUC 1.000, AP 1.000; parameters not reused.

## Eval Reserved

| Level | Count | Positive | Negative | Conflicts |
| --- | ---: | ---: | ---: | ---: |
| Observation | 286 | 255 | 31 | n/a |
| Work any-positive | 217 | 190 | 27 | 3 |

- **Not used in fit:** True

## Next Step

`ml-offline-production-candidate-scoring-v3` may apply this holdout-bound scorer to the reserved product-candidate eval works.

## Not Shadow / Not Production

- This is not validation.
- This is not shadow scoring.
- This is not production scoring.
- No binary model file was written.
- Product-candidate eval works were excluded from training.
- Production defaults remain blocked.

## Caveats

- Not validation.
- Train-only fit on holdout train arm; eval arm reserved for scoring v3.
- In-sample train metrics are diagnostic only.
- Do not equate train in-sample metrics with v1 full-corpus in-sample metrics.
- Product snapshot eval works excluded from this fit.
- No shadow/production authorization.
