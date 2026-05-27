# ml-shadow-scorer-v1 Online Shadow Phase 1 No-Write Pilot Plan (ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-plan-v1)

## Executive Summary

This artifact defines the no-write non-production pilot plan. It does not run the pilot, enable the feature flag, write outputs, or change production behavior.

- Plan defined: True
- Pilot executed: False
- Online shadow execution authorized: True
- Online shadow execution enabled: False
- Writes allowed: False
- Recommended next stage: `implement_online_shadow_phase1_no_write_pilot_runner_v1`

## Phase 1 Scope

- Ranking run: `rank-83787b91ef`
- Family: `emerging`
- Candidate SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`
- Formula: `hybrid_rank_mean_50_50`
- Feature flag: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED`

## No-Write Contract

- No DB writes: True
- No runtime artifact writes: True
- Runtime output capture: process logs/test harness output only for operator review

## Observability

- Policy contract keys: ['component_coverage', 'family_counts', 'latency', 'missing_learned_probability', 'output_completeness', 'rank_displacement', 'runtime_errors', 'score_distributions', 'skipped_candidates_and_reasons', 'skipped_ranking_run_records', 'top_k_overlap_with_heuristic', 'write_counts_by_isolated_target']
- Run-level fields: ['status', 'shadow_row_count', 'writes_performed', 'production_default_changed', 'user_visible_ranking_changed', 'api_web_changes_allowed', 'runtime_feature_flag_value']

## Rollback Disable Drill

- Disable switch: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED=off`
- Verify off before and after pilot.
- Stop pilot jobs on incident or revoke.

## Pass/Fail Criteria

Pass only if:
- non-prod only
- exact identity match
- complete final_score + audit_embedding_probability_work coverage
- no label fields present
- runtime returns rows only in memory
- writes_performed == false
- production/API/user-visible outputs unchanged
- disable drill passes
- all grant-required observability fields recorded for the run

Fail/stop if:
- identity mismatch
- incomplete coverage
- any write attempt
- labels present
- API/prod/default mutation detected
- disable drill fails
- runtime error exceeds threshold
- any required observability field missing

## Blockers

- `api_web_changes_allowed`: False
- `authorization_scope`: bounded_non_prod_pilot_only
- `blockers_changed_by_grant`: ['missing_online_shadow_execution_authorization']
- `blockers_unchanged_by_request`: True
- `missing_generalization_audit_gates`: False
- `missing_generalization_audit_on_second_surface`: False
- `missing_online_shadow_enablement_gates`: False
- `missing_online_shadow_execution_authorization`: False
- `missing_online_shadow_implementation_disabled_by_default`: False
- `missing_production_readiness_authorization`: True
- `missing_shadow_runtime_isolation_verification`: False
- `online_shadow_execution_enabled`: False
- `production_default_allowed`: False
- `runtime_execution_authorized`: True
- `runtime_implementation_authorized`: False
- `shadow_scoring_allowed`: True
- `user_visible_ranking_changed`: False
- `phase1_no_write_pilot_executed`: False

## Out Of Scope

- runtime pilot execution
- enabling ML_SHADOW_SCORER_V1_RUNTIME_ENABLED
- database writes or shadow tables
- API/web integration
- production/default ranking behavior changes
- production readiness authorization
- Phase 2 isolated audit writes

## Caveats

- Plan only; this artifact does not run the pilot.
- Grant is scoped to a non-production pilot only.
- Online shadow execution remains disabled globally until a later pilot runner operates in the approved pilot environment with the flag on.
- No production default or production readiness authorization is granted.
- Phase 1 allows no writes; any Phase 2 isolated audit writes require a separate proof and authorization.
