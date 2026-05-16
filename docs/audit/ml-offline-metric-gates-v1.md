# Offline Metric Gates (ml-offline-metric-gates-v1)

## Executive Summary

- **Experiment scope:** `audit_pool_offline_ranker`
- **Audit ranker gates passed:** True
- **Recommended next stage:** `proceed_to_production_candidate_offline_scoring`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

This is an audit-pool offline gate evaluation over mixed review pools, observation-level labels, text embeddings, classification metrics, and grouped CV. It is not a product-ranking experiment or validation of live recommender quality.

## Headline Ranker Metrics

| Metric | Value |
| --- | ---: |
| Balanced accuracy | 0.854 |
| ROC-AUC | 0.939 |
| Average precision | 0.975 |
| Leakage overlap | 0 |
| Duplicate observation pressure | 85 |

## Gate Checklist

| Gate | Status | Rationale |
| --- | --- | --- |
| `G01_target_scope` | pass | v1 gates only evaluate good_or_acceptable and require surprising_or_useful to remain excluded. |
| `G02_policy_compliance` | pass | The ranker experiment must prove it followed the split policy and wrote no production artifact. |
| `G03_leakage_zero` | pass | No canonical work may appear in both train and eval folds. |
| `G04_minimum_work_groups` | pass | Grouped CV needs at least two positive and two negative canonical work groups. |
| `G05_class_balance_floor` | pass | The audit experiment needs enough observations and grouped positives/negatives to be worth interpreting. |
| `G06_majority_baseline_margin` | pass | The embedding classifier must clear the trivial majority baseline by a meaningful balanced-accuracy margin. |
| `G07_roc_auc_floor` | pass | Every fold must produce evaluable ROC-AUC above the audit threshold. |
| `G08_average_precision_floor` | pass | The embedding classifier must produce strong precision-recall ranking signal on audit folds. |
| `G09_fold_stability` | pass | Audit performance must be reasonably stable across grouped folds. |
| `G10_duplicate_pressure_reported` | pass | Duplicate/conflicting observations may exist; this gate only checks that their pressure is visible. |
| `G11_audit_pool_scope_acknowledged` | pass | The input is a mixed audit-pool experiment, not a product-candidate ranking experiment. |
| `G12_product_candidate_experiment_required` | not_evaluated | Product-shaped candidate pool and ranking-relevant workflow metrics have not been measured by this audit experiment. |
| `G13_production_readiness_plan_alignment` | pass | Audit gates may advance offline research only when the production plan still blocks production eligibility. |
| `G14_transfer_readiness_advisory` | advisory_warn | Transfer-readiness is recorded as advisory context. Audit ranker gates do not treat transfer-readiness as pass/fail evidence in v1. |

## Not Ship / Not Shadow Yet

Passing audit gates may only recommend a production-candidate offline scoring experiment. Shadow scoring remains blocked until audit gates pass, product-candidate experiment gates pass in a future artifact, and `ml-shadow-scorer-v1` exists. Production defaults remain blocked in all v1 outcomes.

## Product-Candidate Experiment Requirements

- product-shaped candidate pool
- ranking-relevant metrics such as top-k precision/recall, PR@k, calibration, and comparison to current heuristic ranking
- candidate pool definition
- no audit-only pool mixing without citation
- no production default change

## Caveats

- Not validation.
- Audit pools only.
- Single-reviewer labels.
- Production blocked.
- Shadow blocked.
- No ranking/API/web changes.

Gate status counts: `{'advisory_warn': 1, 'not_evaluated': 1, 'pass': 12}`
