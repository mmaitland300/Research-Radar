# Offline tiny baseline (emerging only)

Deterministic stratified cross-validation on a **fixed** manual-label slice joined to `paper_scores`. No database writes; no production, API, or web behavior change.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **target:** `surprising_or_useful`
- **family:** `emerging`
- **label_dataset_version:** `ml-label-dataset-v3`
- **label_dataset_sha256:** `ebe1ec0d258d5c2a183ab29c6d6bda570a1bab1ce88f27e10c2c62d3e076fcbd`

## Caveats

- This is an offline tiny baseline experiment, not validation.
- Labels are single-reviewer audit labels with ranking-selection bias.
- Results must not change production ranking defaults.
- No train/dev/test split is created by this artifact.

## Class counts

- **positive:** `43`
- **negative:** `17`
- **total joined:** `60`

## CV policy

- **folds:** `5`
- **model:** `l2_logistic_regression_gradient_descent_pure_python`
- **features:** final_score, semantic_score, citation_velocity_score, topic_growth_score, diversity_penalty

## Out-of-fold aggregate metrics

### Learned (logistic on standardized features)

- **roc_auc_mann_whitney:** `0.8549931600547196`
- **pairwise_accuracy:** `0.8549931600547196`
- **precision_at_5/10/20:** `1.0` / `1.0` / `0.9`

### Heuristic (final_score only, same rows)

- **roc_auc_mann_whitney:** `0.8344733242134063`
- **pairwise_accuracy:** `0.8344733242134063`
- **precision_at_5/10/20:** `1.0` / `1.0` / `0.9`

## Mean coefficients (standardized feature space, averaged across folds)

- **intercept:** `1.5481875749291438`

- **`final_score`:** `0.33140494886762223`
- **`semantic_score`:** `1.4680167880516726`
- **`citation_velocity_score`:** `-0.34812384828876347`
- **`topic_growth_score`:** `0.5577129822168255`
- **`diversity_penalty`:** `0.0`

## Interpretation

This artifact compares a tiny transparent logistic model on five persisted score features against the ranking's final_score on the same cross-validation folds. Stratified CV estimates offline behavior on this fixed slice only; it does not allocate a production held-out split. It does not claim ML superiority over production ranking and is not validation.
