# ML Shadow Scorer v1 Generalization Second Surface (ml-shadow-scorer-v1-generalization-second-surface-v1)

## Executive Summary

This artifact performs read-only source discovery for a distinct second fresh surface. It does not execute the generalization audit or shadow scorer and does not authorize runtime or production behavior.

- Status: `selected_ready_for_generalization_audit`
- Sources considered: 1
- Ready for generalization audit execution: True
- Recommended next stage: `audit_ml_shadow_scorer_v1_on_second_fresh_surface`

## Selected Second Surface

- `ranking_run_id`: rank-83787b91ef
- `family`: emerging
- `corpus_snapshot_version`: source-snapshot-shadow-generalization-v1-20260521
- `embedding_version`: shadow-generalization-text-embedding-v1
- `candidate_pool_work_count`: 528
- `candidate_pool_work_set_sha256`: f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc
- `confirmatory_metric_eligible_work_count`: 168
- `final_score_coverage_count`: 528
- `missing_final_score_count`: 0
- `learned_probability_coverage_count`: 528
- `missing_learned_probability_count`: 0
- `distinct_from_first_validated_surface`: True

## Sources Considered

| Ranking run | Candidate count | Confirmatory eligible | SHA | Status |
| --- | ---: | ---: | --- | --- |
| `rank-83787b91ef` | 528 | 168 | `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc` | distinct |

## Overlap Report

- `old_217_eval_work_set_sha256`: 213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a
- `first_validated_candidate_work_set_sha256`: 927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6
- `old_217_overlap_count`: 217
- `rank_9f4b2a2084_overlap_count`: 358
- `combined_prior_surface_overlap_count`: 360
- `confirmatory_denominator_excludes_prior_overlaps`: True

## Threshold Check

- `minimum_confirmatory_candidate_work_count`: observed `168` / threshold `100` / passed `True`
- `minimum_confirmatory_labeled_work_count`: observed `168` / threshold `100` / passed `True`
- `minimum_confirmatory_positive_work_count`: observed `94` / threshold `50` / passed `True`
- `minimum_confirmatory_negative_work_count`: observed `74` / threshold `20` / passed `True`
- `minimum_distinct_negative_work_count`: observed `74` / threshold `20` / passed `True`
- `minimum_confirmatory_label_coverage_rate`: observed `1.00` / threshold `0.6000` / passed `True`
- `unresolved_label_conflicts`: observed `0` / threshold `0` / passed `True`
- `final_score_coverage`: observed `528` / threshold `528` / passed `True`
- `learned_probability_coverage`: observed `528` / threshold `528` / passed `True`

## Learned Probability Coverage

- `learned_probability_coverage_count`: 528
- `missing_learned_probability_count`: 0
- `approved_upstream_probability_probe`: {'probe_status': 'found', 'source_artifact_path': 'docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json', 'learned_probability_coverage_count': 528, 'full_coverage': True}
- `embedding_coverage_probe`: {'embedding_version': 'shadow-generalization-text-embedding-v1', 'embedding_coverage_count': 528, 'candidate_pool_work_count': 528, 'full_embedding_coverage': True}
- `scorer_execution_used`: False

## Blockers

- `missing_generalization_audit_plan_v1`: False
- `missing_generalization_second_surface_selected`: False
- `missing_generalization_audit_on_second_surface`: True
- `missing_generalization_audit_gates`: True
- `missing_online_shadow_implementation_disabled_by_default`: True
- `missing_shadow_runtime_isolation_verification`: True
- `missing_production_readiness_authorization`: True
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False
- `runtime_implementation_authorized`: False

## Caveats

- Selection/inventory artifact only; does not execute generalization audit or shadow scorer.
- Does not materialize full hybrid surface rows; records metadata for a future surface/audit pass.
- Blocked outcomes are expected and must not invent ranking runs or probabilities.
- No database writes, ranking creation, scorer execution, embedding generation, label ingest, online shadow, API/web, or production changes.
