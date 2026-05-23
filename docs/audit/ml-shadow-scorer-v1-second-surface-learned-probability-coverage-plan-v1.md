# Second-Surface Learned-Probability Coverage Plan (ml-shadow-scorer-v1-second-surface-learned-probability-coverage-plan-v1)

## Executive Summary

This plan freezes the path to achieve full learned-probability coverage for the selected second shadow-generalization surface. It is plan-only: no scorer is applied, no embeddings are generated, no discovery is rerun, and no shadow or production behavior is enabled.

- Ranking run: `rank-83787b91ef`
- Family: `emerging`
- Candidate pool SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`
- Candidate pool: 528
- Confirmatory eligible: 168
- Embedding coverage: 528 / 528
- Learned-probability coverage: 0 / 528
- Label coverage reference: 168 / 168 (94 positive, 74 negative)
- Recommended next stage: `apply_second_surface_learned_probability_coverage_v1`

## Learned-Probability Contract

- Approved scorer: `ml-offline-audit-embedding-scorer-v2`
- Approved embedding version: `shadow-generalization-text-embedding-v1`
- Approved embeddings artifact: `docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json`
- Output field: `audit_embedding_probability_work`
- Target coverage: 528 / 528
- Future execution command: `ml-shadow-scorer-second-surface-learned-probability-apply`
- Future execution artifact: `docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json`
- Must not refit: True
- Must not regenerate embeddings: True
- Must not use v11 labels as scorer features: True

## Future Probe And Discovery

- Probe update: extend discovery _approved_probability_probe to read the new artifact keyed by ranking_run_id + candidate_pool_work_set_sha256; do not reuse first-surface audit artifacts
- Discovery rerun: ml-shadow-scorer-generalization-second-surface with ml-label-dataset-v11.json; expected status selected_ready_for_generalization_audit

## Blocker Semantics Note

Discovery blocker missing_generalization_second_surface_selected means the selected second surface is not yet ready for generalization audit execution; it does not mean no surface was selected. selected_second_surface is populated and readiness_for_generalization_audit.candidate_source_selected is true.

## Remaining Blockers

- `missing_second_surface_learned_probability_coverage`: True
- `missing_generalization_audit_on_second_surface`: True
- `missing_generalization_audit_gates`: True
- `runtime_implementation_authorized`: False
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False
- `blocker_semantics_note`: Discovery blocker missing_generalization_second_surface_selected means the selected second surface is not yet ready for generalization audit execution; it does not mean no surface was selected. selected_second_surface is populated and readiness_for_generalization_audit.candidate_source_selected is true.

## Planned Commit Sequence

1. feat(eval): add second-surface learned-probability coverage plan for shadow generalization v1
2. feat(eval): apply frozen ml-offline-audit-embedding-scorer-v2 to 528 existing embeddings; emit docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json
3. feat(eval): extend discovery probe and rerun second-surface discovery
4. feat(eval): audit ml-shadow-scorer-v1 on second fresh surface

## Explicitly Blocked Actions

- `database_writes`
- `ranking_run_creation`
- `embedding_generation`
- `scorer_refit/training`
- `learned_probability_application_in_this_plan_commit`
- `scorer_execution_in_this_plan_commit`
- `label_ingest`
- `online_shadow_execution`
- `api_web_change`
- `production_default_change`

## Caveats

- Plan artifact only; it does not apply the frozen scorer or write learned probabilities.
- No database access, database writes, embeddings, ranking, discovery rerun, label ingest, shadow runtime, API/web, or production/default changes.
- The v11 labels are metric evidence only and must not be used as scorer features.
- Full learned-probability coverage remains required before the second surface can be ready for generalization audit execution.
