# ml-shadow-scorer-v1 Production Readiness Authorization Criteria v1

## Executive Summary

This artifact opens the production-readiness authorization chain by defining criteria only. It grants no production readiness, production default, API/web, user-visible ranking, database, runtime, refit, embedding, or label-ingest authorization.

- Criteria version: `ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1`
- Production readiness criteria defined: True
- Production readiness authorization requested: False
- Production readiness authorization granted: False
- Missing production readiness authorization: True
- Recommended next stage: `request_production_readiness_authorization_v1`

## Pinned Identity

- ranking_run_id: `rank-83787b91ef`
- family: `emerging`
- corpus_snapshot_version: `source-snapshot-shadow-generalization-v1-20260521`
- embedding_version: `shadow-generalization-text-embedding-v1`
- candidate_pool_work_set_sha256: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`
- formula_id: `hybrid_rank_mean_50_50`
- scorer_id: `ml-shadow-scorer-v1`

## Upstream Accepted Phase 2 Evidence

- Phase bundle: `docs/audit/bundles/phase2-v1/bundle.json`
- Phase bundle SHA-256: `9b077aae115a161580110373a4df722ba4570657cd89d01f994174cc96fdce8d`
- Phase bundle revision: 3
- Phase 2 write pilot reviewed: True
- Phase 2 write pilot accepted: True
- Phase 2 write pilot review decision: `accepted`
- Pilot run id: `rank-83787b91ef-20260528T212715Z`
- Upstream recommended next stage: `begin_production_readiness_authorization_v1`

## Superseded Plan Reconciliation

- Superseded plan path: `docs/audit/ml-production-readiness-plan-v1.md`
- Superseded plan SHA-256: `1fba868797ee84d301bdc20f9facad1602bb3192894956861d5a8214b4680561`
- Superseded by: `ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1`
- The older production-readiness plan predates the accepted Phase 2 shadow ladder.
- Its claims that shadow scoring cannot start or shadow gates are not started are superseded by the accepted Phase 2 isolated-audit write pilot review.
- Production readiness remains blocked despite Phase 2 acceptance.
- This artifact defines criteria only and grants no production behavior.

## Required Production-Readiness Evidence Gates

- multi_reviewer_adjudication_required: True
- label_volume_and_balance_gate_required: True
- leakage_control_review_required: True
- offline_metric_gate_required: True
- calibration_and_threshold_review_required: True
- subgroup_or_slice_regression_review_required: True
- production_scope_rollback_disable_drill_required: True
- production_observability_slo_required: True
- incident_response_and_revocation_plan_required: True
- api_web_default_change_review_required: True
- user_visible_ranking_change_review_required: True
- data_retention_and_auditability_review_required: True

## Explicit Non-Authorizations

- This artifact does not request production readiness authorization.
- This artifact does not grant production readiness authorization.
- This artifact does not authorize production default changes.
- This artifact does not authorize API/web changes.
- This artifact does not authorize user-visible ranking changes.
- This artifact does not authorize DB writes or DDL.
- This artifact does not authorize global online shadow execution.
- This artifact does not authorize model/scorer refits, embedding generation, or label ingest.

## Remaining Blockers

- missing_production_readiness_authorization: True
- production_readiness_authorization_requested: False
- production_readiness_authorization_granted: False
- production_default_allowed: False
- api_web_changes_allowed: False
- user_visible_ranking_changed: False
- online_shadow_execution_enabled: False

## Recommended Next Stage

`request_production_readiness_authorization_v1`

## Caveats

- Criteria artifact only; grants nothing.
- Does not run shadow scoring or any runtime.
- Does not write shadow-runs/ files.
- Does not mutate legacy evidence artifacts.
- Does not enable online shadow execution globally.
- Does not authorize production readiness, production default, API/web, or user-visible ranking changes.
- Supersedes only stale readiness posture language, not historical evidence.
