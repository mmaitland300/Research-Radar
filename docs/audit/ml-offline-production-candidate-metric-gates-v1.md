# Product-Candidate Offline Metric Gates (ml-offline-production-candidate-metric-gates-v1)

## Executive Summary

This evaluates the product-candidate offline diagnostic on the existing `paper_scores` pool. It is read-only reuse, not live recommender validation.

- **Product-candidate heuristic gates passed:** True
- **Learned scorer product-candidate gates passed:** False
- **Recommended next stage:** `create_frozen_audit_embedding_scorer_export_v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

v1 expects `heuristic_and_coverage_only`: heuristic `final_score` evidence may pass, but the learned arm is not evaluated. Passing heuristic gates authorizes only the next offline artifact, `create_frozen_audit_embedding_scorer_export_v1`, not shadow.

## Gate Checklist

| Gate | Title | Status | Required for | Rationale |
| --- | --- | --- | --- | --- |
| `G01_input_scope` | Input Scope | pass | product_candidate_heuristic_gates | The diagnostic must identify the product-candidate pool, ranking run, and family it evaluated. |
| `G02_prior_audit_gates_passed` | Prior Audit Gates Passed | pass | product_candidate_heuristic_gates | Product-candidate scoring may only be interpreted after the audit-pool ranker gates cleared. |
| `G03_candidate_pool_size` | Candidate Pool Size | pass | product_candidate_heuristic_gates | The existing product-candidate pool must contain enough distinct works for a diagnostic gate. |
| `G04_label_coverage` | Label Coverage | pass | product_candidate_heuristic_gates | The product-candidate pool needs broad label overlap and enough labeled works to interpret heuristics. |
| `G05_negative_coverage` | Negative Coverage | pass | product_candidate_heuristic_gates | At least a small negative set is needed so high top-k precision is not the only evidence. |
| `G06_embedding_coverage` | Embedding Coverage | pass | product_candidate_heuristic_gates | The labeled candidate observations must mostly have embeddings before exporting an audit scorer next. |
| `G07_heuristic_roc_auc` | Heuristic ROC-AUC | pass | product_candidate_heuristic_gates | The persisted heuristic score must separate positive and negative labeled works above the floor. |
| `G08_heuristic_average_precision` | Heuristic Average Precision | pass | product_candidate_heuristic_gates | The heuristic final score needs strong precision-recall evidence on the labeled overlap. |
| `G09_top_k_precision` | Top-K Precision | pass | product_candidate_heuristic_gates | Top-10 precision on labeled works must clear the minimum heuristic threshold. |
| `G10_learned_scorer_status` | Learned Scorer Status | not_evaluated | learned_scorer_product_candidate_gates | v1 heuristic_and_coverage_only mode does not evaluate learned product scores; this does not affect product_candidate_heuristic_gates_passed. |
| `G11_shadow_blockers_documented` | Shadow Blockers Documented | pass | product_candidate_heuristic_gates | The gate artifact must explicitly keep shadow blocked even when heuristic gates pass. |
| `G12_production_readiness_alignment` | Production Readiness Alignment | pass | product_candidate_heuristic_gates | Heuristic product-candidate gates may advance only while the production plan remains blocked. |
| `G13_positive_prevalence_advisory` | Positive Prevalence Advisory | advisory_warn | advisory | High P@k may be prevalence-driven on a positive-heavy labeled subset. |

## Product-Candidate Coverage Summary

| Measure | Value |
| --- | ---: |
| Candidate unique works | 217 |
| Candidate label coverage rate | 1.000 |
| Labeled eval works | 217 |
| Labeled positive works | 190 |
| Labeled negative works | 27 |
| Missing embedding rate | 0.000 |

## Heuristic Metric Summary

| Metric | Value |
| --- | ---: |
| ROC-AUC (Mann-Whitney) | 0.804 |
| Average precision | 0.958 |
| Precision@10 | 1.000 |
| Positive work prevalence | 0.876 |

## Learned Scorer Status

- **Gate status:** not_evaluated
- **Scoring mode:** `heuristic_and_coverage_only`
- **Rationale:** v1 heuristic_and_coverage_only mode does not evaluate learned product scores; this does not affect product_candidate_heuristic_gates_passed.

## Positive Prevalence Advisory

The labeled eval subset is positive-heavy, so high P@k may be prevalence-driven. This advisory does not fail heuristic gates.

## Not Shadow / Not Production

- This is not shadow scoring.
- This is not production scoring.
- `shadow_scoring_allowed` is always false in v1.
- `production_default_allowed` is always false in v1.
- No `ml-shadow-scorer-v1` contract exists.
- No production model artifact exists.

## Recommended Next Stage

`create_frozen_audit_embedding_scorer_export_v1`

## Caveats

- Not validation.
- Product-candidate diagnostic only.
- Existing product-candidate pool reused read-only; no new ranking was run.
- Heuristic evidence is not learned model evidence.
- Learned scorer not evaluated in heuristic_and_coverage_only mode.
- Shadow scoring and production default remain blocked.
- No API/web changes.

Gate status counts: `{'advisory_warn': 1, 'not_evaluated': 1, 'pass': 11}`
