# Tiny baseline robustness rollup (emerging only)

Offline-only diagnostics: ablations and fold-wise summaries vs `heuristic_final_score`. No ranking, API, web, or default behavior changes.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **targets:** good_or_acceptable, surprising_or_useful
- **label_dataset_version:** `ml-label-dataset-v3`
- **label_dataset_sha256:** `ebe1ec0d258d5c2a183ab29c6d6bda570a1bab1ce88f27e10c2c62d3e076fcbd`

## Caveats

- This is an offline robustness diagnostic, not validation.
- Labels are single-reviewer audit labels with ranking-selection bias.
- Results must not change production ranking defaults.
- No train/dev/test split is created by this artifact.
- Flat or unchanged P@k means the learned model has not shown improved recommendation-head quality.

## Join summary

- **dual-bool emerging rows joined:** `60`

## Target: `good_or_acceptable`

- **fold fingerprint:** `4ba03100ca45eb46341f176db62f623ff0e4c8097ea56cc1946f7458a90ba212`
- **class counts:** +45 / −15 (n=60)

### Conservative decision fields

- **`oof_p_at_k_unchanged_vs_heuristic`:** `True`
- **`suggested_next_step`:** `Continue offline diagnostics (ablations, fold stability, more labels) before any production experiment.`
- **`supports_more_ml_experiments`:** `True`
- **`supports_product_ranking_change`:** `False`
- **`supports_validation_claim`:** `False`

- **interpretation_summary:** Learned_full improves OOF AUC / pairwise modestly vs heuristic_final_score, but P@k is flat or unchanged; this supports offline feature-learning investigation, not product ranking change. learned_without_final_score is close to learned_full relative to heuristic; there may be signal beyond the existing composite. This is not validation and does not justify production superiority.

### OOF ROC AUC by spec

| spec | OOF AUC | mean fold AUC |
|------|---------|---------------|
| `heuristic_final_score` | `0.9096296296296297` | `0.9111111111111111` |
| `learned_final_score_only` | `0.8740740740740741` | `0.9111111111111111` |
| `learned_semantic_only` | `0.914074074074074` | `0.9481481481481481` |
| `learned_topic_citation_only` | `0.8` | `0.8444444444444444` |
| `learned_without_final_score` | `0.9422222222222222` | `0.9555555555555555` |
| `learned_full` | `0.9407407407407408` | `0.9555555555555555` |

### learned_full vs heuristic (comparison)

- **`aggregate_auc_delta`:** `0.03111111111111109`
- **`aggregate_pairwise_delta`:** `0.03111111111111109`
- **`learned_beat_heuristic_fold_count`:** `3`
- **`learned_lost_to_heuristic_fold_count`:** `0`
- **`learned_tied_heuristic_fold_count`:** `2`
- **`p_at_k_improved_any_fold`:** `True`
- **`p_at_k_worsened_any_fold`:** `True`
- **`worst_fold_auc_gap`:** `0.0`

## Target: `surprising_or_useful`

- **fold fingerprint:** `73b74164ee958bb8bb44edae13eb4611a0e80273e93057735937875e22eb0895`
- **class counts:** +43 / −17 (n=60)

### Conservative decision fields

- **`oof_p_at_k_unchanged_vs_heuristic`:** `True`
- **`suggested_next_step`:** `Continue offline diagnostics (ablations, fold stability, more labels) before any production experiment.`
- **`supports_more_ml_experiments`:** `True`
- **`supports_product_ranking_change`:** `False`
- **`supports_validation_claim`:** `False`

- **interpretation_summary:** Learned_full improves OOF AUC / pairwise modestly vs heuristic_final_score, but P@k is flat or unchanged; this supports offline feature-learning investigation, not product ranking change. learned_without_final_score is close to learned_full relative to heuristic; there may be signal beyond the existing composite. This is not validation and does not justify production superiority.

### OOF ROC AUC by spec

| spec | OOF AUC | mean fold AUC |
|------|---------|---------------|
| `heuristic_final_score` | `0.8344733242134063` | `0.850925925925926` |
| `learned_final_score_only` | `0.7852257181942545` | `0.850925925925926` |
| `learned_semantic_only` | `0.8768809849521204` | `0.9092592592592592` |
| `learned_topic_citation_only` | `0.725718194254446` | `0.774074074074074` |
| `learned_without_final_score` | `0.8549931600547196` | `0.8870370370370371` |
| `learned_full` | `0.8549931600547196` | `0.8870370370370371` |

### learned_full vs heuristic (comparison)

- **`aggregate_auc_delta`:** `0.02051983584131334`
- **`aggregate_pairwise_delta`:** `0.02051983584131334`
- **`learned_beat_heuristic_fold_count`:** `3`
- **`learned_lost_to_heuristic_fold_count`:** `0`
- **`learned_tied_heuristic_fold_count`:** `2`
- **`p_at_k_improved_any_fold`:** `True`
- **`p_at_k_worsened_any_fold`:** `True`
- **`worst_fold_auc_gap`:** `0.0`
