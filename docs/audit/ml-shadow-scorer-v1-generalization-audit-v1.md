# ML Shadow Scorer v1 Generalization Audit Plan (ml-shadow-scorer-v1-generalization-audit-v1)

## Executive Summary

This plan defines the second-surface generalization audit required before any online shadow runtime implementation. It is plan-only and does not execute scoring, materialize a surface, or authorize runtime behavior.

- Generalization audit plan defined: True
- Generalization audit executed: False
- Runtime implementation authorized: False
- Online shadow execution enabled: False
- Recommended next stage: `materialize_or_select_second_fresh_surface_for_shadow_generalization_v1`

## Why Generalization Is Required

The existing evidence is bound to one fresh surface. A second fresh surface must show the frozen formula behaves well outside `rank-9f4b2a2084` before runtime implementation can be considered.

## Existing Validated Surface

- Ranking run: `rank-9f4b2a2084`
- Family: `emerging`
- Snapshot: `source-snapshot-fresh-hybrid-v1-20260518`
- Embedding version: `fresh-hybrid-text-embedding-v1`
- Candidate SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`
- Can be reused for generalization evidence: False

## Second-Surface Requirements

- Candidate SHA must differ from `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`
- Ranking run must differ from `rank-9f4b2a2084`
- Snapshot should differ from `source-snapshot-fresh-hybrid-v1-20260518` unless justified
- Frozen formula: `hybrid_rank_mean_50_50`
- Full final_score coverage required: True
- Full learned-probability coverage required: True

## Label Thresholds

- `work_level_target`: good_or_acceptable
- `minimum_confirmatory_labeled_work_count`: 100
- `minimum_confirmatory_positive_work_count`: 50
- `minimum_confirmatory_negative_work_count`: 20
- `minimum_distinct_negative_work_count`: 20
- `minimum_confirmatory_label_coverage_rate`: 0.6
- `minimum_confirmatory_candidate_work_count`: 100
- `label_conflicts_must_be_reported`: True
- `unresolved_conflicts_block_future_gate_pass`: True
- `labels_used_for_metric_evaluation_only`: True
- `labels_used_as_scoring_features`: False

## Metric Requirements

- Primary arm: `hybrid_rank_mean_50_50`
- Baseline: `heuristic final_score`
- Metric denominator: confirmatory-eligible labeled works only
- Material lift: delta ROC-AUC >= 0.03 OR delta AP >= 0.02

## Scorer Replay Requirements

- `formula`: 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)
- `formula_id`: hybrid_rank_mean_50_50
- `rank_pct_scope`: full second-surface candidate pool
- `higher_is_better`: True
- `ties`: average-rank ties
- `audit_embedding_probability_work_source`: already exists or approved upstream application of frozen ml-offline-audit-embedding-scorer-v2 to pre-existing embeddings
- `embedding_generation_inside_audit_allowed`: False
- `learned_scorer_refit_allowed`: False
- `learned_probability_creation_inside_audit_allowed`: False
- `weight_tuning_allowed`: False
- `incomplete_learned_probability_coverage_action`: block execution or emit blocked/skipped artifact per policy skip contract

## Overlap Reporting

- `report_old_217_eval_set_overlap`: True
- `report_rank_9f4b2a2084_surface_overlap`: True
- `report_combined_prior_surface_overlap`: True
- `report_sha_level_overlap`: True
- `confirmatory_denominators_exclude_previous_eval_overlap`: True

## Future Gate Contract

- second surface requirements pass
- label requirements pass
- scorer replay spec-compliant and exact where replay targets exist
- no leakage/refit/tuning/embedding generation inside audit
- material lift passes vs heuristic on second surface
- shadow/prod/runtime remain blocked

## Planned Commit Sequence

1. feat(eval): draft ml-shadow-scorer-v1 generalization audit v1 (this commit)
2. feat(eval): materialize or select second fresh surface for shadow generalization v1
3. feat(eval): audit ml-shadow-scorer-v1 on second fresh surface
4. feat(eval): add ml-shadow-scorer-v1 generalization audit gates
5. feat(eval): implement online shadow runtime disabled by default

## Blockers

- `missing_generalization_audit_plan_v1`: False
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

- Plan only; no generalization audit execution occurs.
- No second surface is materialized or selected by this command.
- No online shadow runtime, API/web behavior, production default, scorer execution, ranking, training, embeddings, or label ingest is authorized.
- First-surface evidence does not generalize until a second-surface audit and gates pass.
