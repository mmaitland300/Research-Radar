# Offline Audit Embedding Scorer Export (ml-offline-audit-embedding-scorer-v1)

## Executive Summary

Frozen JSON scorer fit on the full eligible audit-labeled embedding corpus for `good_or_acceptable`. It is intended only for a later offline product-candidate scoring pass.

- **Target:** `good_or_acceptable`
- **Fit mode:** `full_fit_audit_corpus`
- **Shadow scoring authorized:** False
- **Production artifact written:** False

## Training Inventory

- **Eligible observations:** 427
- **Unique canonical works:** 342
- **Duplicate observation pressure:** 85
- **Conflicting target work groups:** 3

| Level | Positive | Negative |
| --- | ---: | ---: |
| Observation | 309 | 118 |
| Work any-positive | 228 | 114 |

## Frozen Scorer Parameters

- **Dimensions:** 1536
- **Classes:** `[False, True]`
- **Random seed:** 20260515
- **Pipeline:** `StandardScaler(with_mean=True) -> LogisticRegression(solver='lbfgs', penalty='l2', max_iter=5000)`

## Apply Instructions

1. Load the JSON scorer.
2. For each embedding vector, compute `z = (x - mean) / scale` per dimension.
3. Compute `logit_true = dot(coefficients_standardized_space, z) + intercept_standardized_space`.
4. Compute `probability_true = sigmoid(logit_true)`.
5. Use `probability_true >= 0.5` only as an offline diagnostic boolean prediction.

## In-Sample Training Metrics

**IN-SAMPLE FULL-FIT ONLY - NOT VALIDATION**

| Metric | Value |
| --- | ---: |
| Accuracy | 0.993 |
| Balanced accuracy | 0.990 |
| Macro F1 | 0.991 |
| ROC-AUC | 1.000 |
| Average precision | 1.000 |
| TN | 116 |
| FP | 2 |
| FN | 1 |
| TP | 308 |

## CV Baseline Reference

- **Source:** `docs/audit/ml-offline-ranker-experiment-v1.json`
- **Coefficient reuse:** none; per-fold coefficients are not reused or averaged.

| CV metric | Mean | Std |
| --- | ---: | ---: |
| Balanced accuracy | 0.854 | 0.031 |
| ROC-AUC | 0.939 | 0.023 |
| Average precision | 0.975 | 0.010 |

## Not Shadow / Not Production

- This is not shadow scoring.
- This is not production scoring.
- This is not validation.
- No binary model file was written.
- No product-candidate pool was used for training.
- Production defaults remain blocked.

## Next Authorized Step

Regenerate production-candidate scoring with the learned audit scorer on the existing pool.

## Caveats

- Not validation.
- Full-fit on audit-labeled corpus only; not product-candidate training.
- In-sample training metrics are diagnostic only.
- Grouped CV leakage controls from ranker experiment do not apply to this full-fit artifact.
- Does not authorize shadow scoring or production default.
- Heuristic final_score and learned audit scorer are separate evidence lines.
- No ranking/API/web changes.
- JSON scorer is an offline audit artifact, not a production model artifact.
