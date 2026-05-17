# Product-Candidate Holdout Metric Gates (ml-offline-production-candidate-metric-gates-v3)

## Executive Summary

This evaluates the v3 product-candidate offline diagnostic where the holdout-bound audit embedding scorer was applied to the reserved product-candidate eval arm.

- **Product-candidate heuristic gates passed:** True
- **Held-out learned validity passed:** True
- **Heuristic non-regression passed:** True
- **Material lift passed:** False
- **Independent learned validation passed:** True
- **Recommended next stage:** `create_hybrid_scorer_offline_experiment_v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

Passing held-out learned validity does not authorize shadow. Material lift is the extra bar before shadow-spec work; on this snapshot the next step is a hybrid offline experiment, not `ml-shadow-scorer-v1`.

## Three-Decision Table

| Decision | Result | Meaning |
| --- | --- | --- |
| Held-out learned validity | True | Minimum eval-only learned floors cleared. |
| Heuristic non-regression | True | Learned ROC-AUC/AP did not regress versus heuristic final_score. |
| Material lift | False | Learned scorer clears the extra lift bar for shadow-spec work. |

## Gate Checklist

| Gate | Title | Status | Required for | Rationale |
| --- | --- | --- | --- | --- |
| `G01_input_scope` | Input Scope | pass | product_candidate_heuristic_gates | The v3 diagnostic must identify the holdout-scored product-candidate pool, ranking run, and family. |
| `G02_prior_audit_gates_passed` | Prior Audit Gates Passed | pass | product_candidate_heuristic_gates | The audit-ranker evidence line must have passed before interpreting product-candidate diagnostics. |
| `G03_candidate_pool_size` | Candidate Pool Size | pass | product_candidate_heuristic_gates | The product-candidate eval arm must contain enough distinct works for deterministic gates. |
| `G04_label_coverage` | Label Coverage | pass | product_candidate_heuristic_gates | The holdout diagnostic needs broad label overlap and enough labeled eval works. |
| `G05_negative_coverage` | Negative Coverage | pass | product_candidate_heuristic_gates | Negative labeled eval works are required to interpret held-out separation metrics. |
| `G06_embedding_coverage` | Embedding Coverage | pass | product_candidate_heuristic_gates | The holdout learned scorer can only be evaluated if labeled eval observations have embeddings. |
| `G07_heuristic_roc_auc` | Heuristic ROC-AUC | pass | product_candidate_heuristic_gates | The heuristic final_score line remains a required baseline evidence check. |
| `G08_heuristic_average_precision` | Heuristic Average Precision | pass | product_candidate_heuristic_gates | The heuristic final_score line should retain strong precision-recall evidence on the same eval works. |
| `G09_heuristic_top_k_precision` | Heuristic Top-K Precision | pass | product_candidate_heuristic_gates | Top-10 precision on labeled eval works must clear the heuristic floor. |
| `G10_holdout_learned_validity` | Holdout Learned Validity | pass | held_out_learned_validity | Held-out diagnostic validity passed means minimum eval-only floors cleared; it does not authorize shadow. |
| `G11_holdout_scorer_provenance` | Holdout Scorer Provenance | pass | held_out_learned_validity | The supplied scorer must be the exact holdout-bound v2 scorer used by scoring v3. |
| `G12_holdout_assignment_alignment` | Holdout Assignment Alignment | pass | held_out_learned_validity | The candidate pool and metric rows must align exactly to the reserved eval work set with no train leakage. |
| `G13_heuristic_non_regression` | Heuristic Non-Regression | pass | heuristic_non_regression | The holdout learned scorer should not underperform heuristic final_score on ROC-AUC or AP. |
| `G14_material_lift` | Material Lift | advisory_warn | material_lift | Material lift is required before considering shadow-spec work; failing it does not fail independent validation. |
| `G15_shadow_blockers_documented` | Shadow Blockers Documented | pass | product_candidate_heuristic_gates | The v3 gate artifact must keep shadow blocked when material lift is insufficient. |
| `G16_production_readiness_alignment` | Production Readiness Alignment | pass | product_candidate_heuristic_gates | Offline validation gates may advance only while production remains explicitly blocked. |
| `G17_positive_prevalence_advisory` | Positive Prevalence Advisory | advisory_warn | advisory | Positive-heavy eval can inflate P@k, so top-k precision should be interpreted cautiously. |

## Coverage

| Measure | Value |
| --- | ---: |
| Candidate unique works | 217 |
| Candidate label coverage rate | 1.000 |
| Labeled eval works | 217 |
| Labeled positive works | 190 |
| Labeled negative works | 27 |
| Missing embedding rate | 0.000 |

## Heuristic vs Holdout Learned Comparison

| Metric | Heuristic final_score | Holdout learned scorer | Delta |
| --- | ---: | ---: | ---: |
| ROC-AUC | 0.804 | 0.805 | 0.001 |
| Average precision | 0.958 | 0.967 | 0.009 |
| Precision@10 | 1.000 | 1.000 | 0.000 |

## v2 Full-Fit Caution

v3 holdout learned ROC is about 0.805 vs heuristic about 0.804; v2 full-fit learned ROC 1.0 was overlap-inflated.

## Leakage and Holdout Alignment

- **Eval work-set SHA:** `213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a`
- Train rows used in metrics: `0` required.
- Train works used in metrics: `0` required.
- Candidate pool work set must match the reserved eval work set.

## Advisories

- `G14_material_lift`: advisory_warn - Material lift is insufficient on the current snapshot.
- `G17_positive_prevalence_advisory`: advisory_warn - Positive-heavy eval can inflate P@k, so top-k precision should be interpreted cautiously.

## Not Shadow / Not Production

- This is not shadow scoring.
- This is not production scoring.
- Passing independent offline validation does not authorize shadow.
- Material lift is insufficient for shadow-spec work on this snapshot.
- No `ml-shadow-scorer-v1` contract exists.
- No production model artifact exists.
- No ranking/API/web changes were made.

## Recommended Next Stage

`create_hybrid_scorer_offline_experiment_v1`

## Caveats

- Not live recommender validation.
- Single-reviewer audit labels.
- One ranking run/family.
- Holdout is relative to scorer v2 train works.
- Positive-heavy eval inflates P@k.
- Embedding-only model does not materially beat final_score on current snapshot.
- No shadow/production authorization.

Gate status counts: `{'advisory_warn': 2, 'pass': 15}`
