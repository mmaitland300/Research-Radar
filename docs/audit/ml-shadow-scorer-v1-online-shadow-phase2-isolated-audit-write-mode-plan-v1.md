# ml-shadow-scorer-v1 Online Shadow Phase 2 Isolated Audit Write-Mode Plan (ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1)

## Executive Summary

This artifact defines the future isolated audit-write contract after the accepted Phase 1 no-write pilot. It does not run the write path, create shadow-run data, enable the feature flag, or authorize Phase 2 writes.

- Plan defined: True
- Plan executed: False
- Phase 2 writes authorized: False
- Phase 2 proof executed: False
- Recommended next stage: `implement_online_shadow_phase2_isolated_audit_write_mode_proof_v1`

## Primary Target

- Target: isolated audit artifact tree only
- Root path: `docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/`
- Pilot run path pattern: `docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/<pilot_run_id>/`
- Deferred DB table path is not authorized by this plan.

## Allowed Writes

- Future writes allowed now: False
- Future proof root: `docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/`
- Only isolated audit shadow artifacts may be positive in the future proof write counts.

## Forbidden Writes

- ranking_runs
- production/default pins
- paper_scores used for production
- API-visible tables
- production config
- labels
- embeddings
- scorer artifacts
- user-visible paths

## Proof Requirements

- Writes occur only under docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/
- All forbidden write_counts remain zero.
- Observability complete including write_counts_by_isolated_target.
- Preflight checklist satisfied.
- Rollback drill disables flag, verifies no further writes, and cleans up pilot subdirectory.
- production_default_changed == false
- user_visible_ranking_changed == false
- api_web_changes_allowed == false

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
- `missing_phase1_no_write_pilot_review`: False
- `missing_phase2_write_mode_isolation_proof`: True
- `missing_production_readiness_authorization`: True
- `missing_shadow_runtime_isolation_verification`: False
- `online_shadow_execution_enabled`: False
- `phase1_no_write_pilot_executed`: True
- `phase2_writes_authorized`: False
- `production_default_allowed`: False
- `runtime_execution_authorized`: True
- `runtime_implementation_authorized`: False
- `shadow_scoring_allowed`: True
- `user_visible_ranking_changed`: False
- `phase2_isolated_audit_write_mode_plan_defined`: True

## Caveats

- Plan only; no writes, no directory creation with data, no runtime persistence, and no feature flag enablement.
- Does not authorize Phase 2 writes or clear missing_phase2_write_mode_isolation_proof.
- Does not enable online shadow globally or change production/API/user-visible ranking.
- Production readiness remains separate.
- Primary target is audit file tree only; DB table path is deferred.
