# Offline tiny baseline (emerging only)

Deterministic stratified cross-validation on a **fixed** manual-label slice joined to `paper_scores`. No database writes; no production, API, or web behavior change.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **target:** `good_or_acceptable`
- **family:** `emerging`
- **label_dataset_version:** `ml-label-dataset-v3`
- **label_dataset_sha256:** `ebe1ec0d258d5c2a183ab29c6d6bda570a1bab1ce88f27e10c2c62d3e076fcbd`

## Caveats

- This is an offline tiny baseline experiment, not validation.
- Labels are single-reviewer audit labels with ranking-selection bias.
- Results must not change production ranking defaults.
- No train/dev/test split is created by this artifact.

## Class counts

- **positive:** `45`
- **negative:** `15`
- **total joined:** `60`

## CV policy

- **folds:** `5`
- **model:** `l2_logistic_regression_gradient_descent_pure_python`
- **features:** final_score, semantic_score, citation_velocity_score, topic_growth_score, diversity_penalty

## Out-of-fold aggregate metrics

### Learned (logistic on standardized features)

- **roc_auc_mann_whitney:** `0.9407407407407408`
- **pairwise_accuracy:** `0.9407407407407408`
- **precision_at_5/10/20:** `1.0` / `1.0` / `1.0`

### Heuristic (final_score only, same rows)

- **roc_auc_mann_whitney:** `0.9096296296296297`
- **pairwise_accuracy:** `0.9096296296296297`
- **precision_at_5/10/20:** `1.0` / `1.0` / `1.0`

## Mean coefficients (standardized feature space, averaged across folds)

- **intercept:** `4.010699501496763`

- **`final_score`:** `1.1762781154504238`
- **`semantic_score`:** `1.9404238042598512`
- **`citation_velocity_score`:** `-1.4452481771521355`
- **`topic_growth_score`:** `2.2972697117003524`
- **`diversity_penalty`:** `0.0`

## Interpretation

This artifact compares a tiny transparent logistic model on five persisted score features against the ranking's final_score on the same cross-validation folds. Stratified CV estimates offline behavior on this fixed slice only; it does not allocate a production held-out split. It does not claim ML superiority over production ranking and is not validation.
