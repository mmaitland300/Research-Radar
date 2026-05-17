# Product-Candidate Learned Metric Gates (ml-offline-production-candidate-metric-gates-v2)

## Executive Summary

This evaluates the v2 product-candidate offline diagnostic where the frozen audit embedding scorer was applied to the existing labeled product-candidate overlap.

The learned audit scorer was full-fit on the audit-labeled v8 corpus (`ml-offline-audit-embedding-scorer-v1`). The v2 product-candidate labeled overlap uses the same label dataset and embedding rows by `row_id` as that training universe. Strong learned metrics here prove successful scorer application and minimum diagnostic separation on the overlap; they are not independent validation and not shadow readiness.

- **Product-candidate heuristic gates passed:** True
- **Learned scorer application gates passed:** True
- **Independent learned validation passed:** False
- **Recommended next stage:** `create_learned_scorer_holdout_policy_v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

Passing v2 gates authorizes defining a learned-scorer holdout/split policy next. It does not authorize shadow implementation.

## Gate Checklist

| Gate | Title | Status | Required for | Rationale |
| --- | --- | --- | --- | --- |
| `G01_input_scope` | Input Scope | pass | product_candidate_heuristic_gates | The v2 diagnostic must identify the learned-scored product-candidate pool, ranking run, and family. |
| `G02_prior_audit_gates_passed` | Prior Audit Gates Passed | pass | product_candidate_heuristic_gates | The audit-ranker evidence line must have passed before interpreting product-candidate diagnostics. |
| `G03_candidate_pool_size` | Candidate Pool Size | pass | product_candidate_heuristic_gates | The existing product-candidate pool must contain enough distinct works for a diagnostic gate. |
| `G04_label_coverage` | Label Coverage | pass | product_candidate_heuristic_gates | The learned application diagnostic needs broad labeled overlap and enough labeled works. |
| `G05_negative_coverage` | Negative Coverage | pass | product_candidate_heuristic_gates | Negative labeled works are required to interpret separation metrics beyond positive-heavy top-k evidence. |
| `G06_embedding_coverage` | Embedding Coverage | pass | product_candidate_heuristic_gates | The learned scorer can only be evaluated if labeled candidate observations mostly have embeddings. |
| `G07_heuristic_roc_auc` | Heuristic ROC-AUC | pass | product_candidate_heuristic_gates | The existing heuristic final_score line should still clear its minimum diagnostic separation floor. |
| `G08_heuristic_average_precision` | Heuristic Average Precision | pass | product_candidate_heuristic_gates | The heuristic final_score line remains a separate minimum evidence check. |
| `G09_heuristic_top_k_precision` | Heuristic Top-K Precision | pass | product_candidate_heuristic_gates | Top-10 precision on labeled works must clear the minimum heuristic threshold. |
| `G10_learned_scorer_application` | Learned Scorer Application | pass | learned_scorer_application_gates | The frozen scorer was applied with minimum diagnostic floors on the labeled overlap; this is not independent validation. |
| `G11_scorer_provenance` | Scorer Provenance | pass | learned_scorer_application_gates | The scoring artifact must point to the exact frozen audit scorer export that is supplied here. |
| `G12_independent_validation_status` | Independent Validation Status | not_evaluated | advisory | The v2 overlap uses the same label dataset and embedding rows as the audit-training universe; these metrics are not holdout validation. |
| `G13_shadow_blockers_documented` | Shadow Blockers Documented | pass | product_candidate_heuristic_gates | The gate artifact must explicitly keep shadow blocked even when learned application gates pass. |
| `G14_production_readiness_alignment` | Production Readiness Alignment | pass | product_candidate_heuristic_gates | Offline product-candidate gates may advance only while the production plan remains blocked. |
| `G15_positive_prevalence_advisory` | Positive Prevalence Advisory | advisory_warn | advisory | High P@k may be prevalence-driven on a positive-heavy labeled subset. |
| `G16_near_perfect_learned_metrics_advisory` | Near-Perfect Learned Metrics Advisory | advisory_warn | advisory | Near-perfect learned metrics may reflect audit-corpus training overlap, not generalization. |

## Coverage

| Measure | Value |
| --- | ---: |
| Candidate unique works | 217 |
| Candidate label coverage rate | 1.000 |
| Labeled eval works | 217 |
| Labeled positive works | 190 |
| Labeled negative works | 27 |
| Missing embedding rate | 0.000 |

## Heuristic Summary

| Metric | Value |
| --- | ---: |
| ROC-AUC (Mann-Whitney) | 0.804 |
| Average precision | 0.958 |
| Precision@10 | 1.000 |
| Positive work prevalence | 0.876 |

## Learned Application Summary

| Metric | Value |
| --- | ---: |
| Score name | `audit_embedding_probability_work` |
| Metric level | `canonical_work_labeled_eval_subset` |
| ROC-AUC (Mann-Whitney) | 1.000 |
| Average precision | 1.000 |
| Precision@10 | 1.000 |

## Heuristic vs Learned Comparison

| Delta | Value |
| --- | ---: |
| ROC-AUC delta | 0.196 |
| Average precision delta | 0.042 |
| Precision@10 delta | 0.000 |

## Independent Validation Status

- **Gate status:** not_evaluated
- The labeled overlap is not an independent holdout because it uses the same audit-labeled corpus and embedding rows as the full-fit scorer training universe.
- Shadow scoring and production default remain blocked until an independent learned validation policy and artifact exist.

## Advisories

- `G15_positive_prevalence_advisory`: High P@k may be prevalence-driven on a positive-heavy labeled subset.
- `G16_near_perfect_learned_metrics_advisory`: Near-perfect learned metrics may reflect audit-corpus training overlap, not generalization.

## Not Shadow / Not Production

- This is not shadow scoring.
- This is not production scoring.
- No `ml-shadow-scorer-v1` contract exists.
- No production model artifact exists.
- No ranking/API/web changes were made.

## Recommended Next Stage

`create_learned_scorer_holdout_policy_v1`

## Caveats

- Not validation.
- Product-candidate offline diagnostic only.
- Learned scorer trained on audit corpus; labeled overlap not independent holdout.
- Near-perfect learned metrics may reflect training overlap.
- Heuristic final_score and learned audit scorer are separate evidence lines.
- Shadow and production default blocked.
- No ranking/API/web changes.

Gate status counts: `{'advisory_warn': 2, 'not_evaluated': 1, 'pass': 13}`
