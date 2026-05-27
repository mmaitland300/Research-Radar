# ml-shadow-scorer-v1 Online Shadow Execution Authorization Grant (ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1)

## Executive Summary

This artifact records a bounded non-production pilot grant for ml-shadow-scorer-v1 online shadow execution. It does not run the runtime, enable the feature flag, write DB rows, change API/web behavior, or affect production defaults.

- Decision: `granted`
- Owner: Matt Maitland
- Review by: 2026-08-27
- Expiry date: 2026-08-27
- Online shadow execution authorized: True
- Online shadow execution enabled: False
- Recommended next stage: `prepare_online_shadow_phase1_no_write_pilot_plan_v1`

## Pilot Scope

- Pilot-only ml-shadow-scorer-v1 online shadow on second surface rank-83787b91ef / emerging / candidate SHA f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc / hybrid_rank_mean_50_50; non-prod only; read-only prod inputs; skip incomplete coverage; no production default/API/user-visible ranking changes.
- Environments: non-prod pilot only
- Initial ranking run ids: ['rank-83787b91ef']

## Write Mode

- Phase 1: no_writes
- Phase 2: isolated_audit_only_writes_after_phase1_and_write_mode_proof

## Rollback

- Disable switch: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED=off`
- Stop pilot jobs on revoke or incident.
- Production ranking remains unchanged.

## Production Separation

- This grant does not grant production default, production readiness, API/web behavior, or user-visible ranking changes.

## Blockers

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
- `runtime_execution_authorized`: False
- `runtime_implementation_authorized`: False
- `shadow_scoring_allowed`: False
- `blockers_changed_by_grant`: ['missing_online_shadow_execution_authorization']

## Caveats

- Grant artifact only; it does not run the runtime or enable a feature flag.
- Authorization is bounded to a non-production pilot on the approved second-surface identity.
- Phase 1 allows no writes.
- Isolated audit-only writes require Phase 1 success and separate write-mode isolation proof for Phase 2.
- This grant does not authorize production default, production readiness, API/web behavior, or user-visible ranking changes.
- Flag default remains off outside the approved pilot environment.
