# Offline bridge recommendable scorer v3 regularization sensitivity v1

Offline-only sweep over the existing v3 `bridge_recommendable` diagnostic. No serving, API, ranking, or database-write behavior is changed.

## Baseline reference

- v3 baseline path: `docs/audit/ml-offline-bridge-recommendable-scorer-v3.json`
- v3 baseline sha256: `2369b68a17f00eda3119ddfa85a27c95fac388c78b471736bc311b9fc5b56ea9`
- Baseline C: 1.0

## Selection

- ready_for_offline_hybrid_eval: False
- selected_frozen_coefficient_C: None
- selection reason: no_C_met_oof_auc_and_overfit_gap_gates

## Sweep

| C | OOF ROC AUC | OOF AP | OOF P@20 | In-sample ROC AUC | Gap | v2-set ROC AUC | Acceptable | Too strong |
|---:|---:|---:|---:|---:|---:|---:|---|---|
| 1.0 | 0.7175757575757575 | 0.7590608599981735 | 0.85 | 1.0 | 0.28242424242424247 | 0.6670673076923076 | False | False |
| 0.1 | 0.7224242424242424 | 0.7635183309688128 | 0.9 | 1.0 | 0.2775757575757576 | 0.6742788461538461 | False | False |
| 0.01 | 0.7386666666666667 | 0.7712094589123856 | 0.85 | 1.0 | 0.2613333333333333 | 0.7011217948717949 | False | False |

## Targeted verdicts

### C=1.0

- high_ml_low_bridge_score: partial
- high_bridge_score_low_ml: fails
- promoted_by_hybrid: supports_hybrid_promotion
- demoted_by_hybrid: separates_competitive_demotions_from_correct_rejections

### C=0.1

- high_ml_low_bridge_score: learns_social_platform_bridge_signal
- high_bridge_score_low_ml: fails
- promoted_by_hybrid: supports_hybrid_promotion
- demoted_by_hybrid: separates_competitive_demotions_from_correct_rejections

### C=0.01

- high_ml_low_bridge_score: learns_social_platform_bridge_signal
- high_bridge_score_low_ml: fails
- promoted_by_hybrid: supports_hybrid_promotion
- demoted_by_hybrid: separates_competitive_demotions_from_correct_rejections

## Caveats

- This is an offline regularization sensitivity diagnostic only.
- It does not enable Bridge serving or production output.
- It does not mutate the committed v3 baseline artifact.
- This is not validation.
- Primary training/evaluation uses deduped 130 unique work_ids; row-level 160-row readouts are audit-only.
- OOF CV metrics on the deduped slice are not in-sample metrics.
- Stratified deduped metrics reuse OOF probabilities from the deduped 130-row CV.
- Row-level stratified readouts map deduped OOF probabilities and are duplicate/conflict sensitive.
- Derived-target conflict on W4415316343 is reported; shadow-pilot row wins dedupe priority.
- No ranking, API, DB-write, shadow rollout, or production serving changes are made or authorized.
