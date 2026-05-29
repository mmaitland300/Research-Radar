# ml-shadow-scorer-v1 Production Readiness Bundle (online-shadow-production-readiness-v1)

## Executive Summary

This bundle is the canonical production-readiness ladder view. It records a request for production-readiness authorization while granting nothing and preserving all production/API/default/user-visible blockers.

- Bundle revision: 2
- Production readiness authorization requested: True
- Production readiness authorization granted: True
- Missing production readiness authorization: False
- Online shadow execution enabled: False
- Recommended next stage: `begin_production_scoped_online_shadow_plan_v1`

## Pinned Identity

- candidate_pool_work_set_sha256: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`
- corpus_snapshot_version: `source-snapshot-shadow-generalization-v1-20260521`
- embedding_version: `shadow-generalization-text-embedding-v1`
- family: `emerging`
- formula_id: `hybrid_rank_mean_50_50`
- ranking_run_id: `rank-83787b91ef`
- scorer_id: `ml-shadow-scorer-v1`

## Legacy Artifact Index

| Role | Path | SHA-256 |
| --- | --- | --- |
| production_readiness_criteria | `docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json` | `3dfc2e2d9cac7693c39383c5238fa9feb536bed8678220f44e5b88c0795e3c00` |
| phase2_bundle | `docs/audit/bundles/phase2-v1/bundle.json` | `9b077aae115a161580110373a4df722ba4570657cd89d01f994174cc96fdce8d` |
| generalization_audit_gates | `docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json` | `f76345f49d2077008e09fbc921b3c51e4483778422539e0936aead74691e2c84` |
| online_shadow_policy | `docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json` | `726b790d1539a7ea158c484c7c374ae3f002f3e0f8fffa4238d3c73fca30e378` |
| execution_authorization_grant | `docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json` | `e9505ebad2033598c6b8f923a2cfc58f362154a1076a2e3014ebafa7d23525f8` |
| production_readiness_plan | `docs/audit/ml-production-readiness-plan-v1.json` | `9fa5eabd0c4fbe120cc7797d944d8545668ee37f2de876263d114c3ef57227fc` |

## Criteria Reference

- Criteria: `docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json`
- Criteria SHA-256: `3dfc2e2d9cac7693c39383c5238fa9feb536bed8678220f44e5b88c0795e3c00`
- Criteria defined: True
- Upstream next stage: `request_production_readiness_authorization_v1`

## Phase 2 Evidence

- Phase 2 pilot accepted: True
- Pilot run id: `rank-83787b91ef-20260528T212715Z`

## Gate Assessments

| Gate | Status | Satisfies Detail | Rationale |
| --- | --- | --- | --- |
| `api_web_default_change_review_required` | `blocked_separate_chain` | True | This request explicitly excludes production default and API/web changes; any such change requires a separate authorization chain. |
| `calibration_and_threshold_review_required` | `satisfied_by_grant` | True | Grant defines bounded production-calibration review criteria for the prod-scoped shadow chain; no threshold, default, API, or user-visible behavior is enabled here. |
| `data_retention_and_auditability_review_required` | `satisfied_by_grant` | True | Grant records retention and auditability expectations: bundle path+SHA references, durable pilot hashes, and frozen legacy evidence. |
| `incident_response_and_revocation_plan_required` | `satisfied_by_grant` | True | Grant records incident and revocation summary derived from the execution grant and online shadow policy: flag-off first, stop jobs, supersede or deny to revoke. |
| `label_volume_and_balance_gate_required` | `satisfied_by_grant` | True | Grant documents 528-row second-surface coverage and carries known label-balance gaps as explicit prod-scoped shadow-chain evidence requirements. |
| `leakage_control_review_required` | `satisfied_by_upstream` | True | Phase 2 execution records labels_used_for_scoring=false, and upstream generalization gates document the second-surface gate context. |
| `multi_reviewer_adjudication_required` | `satisfied_by_grant` | True | Production-readiness grant satisfies adjudication using owner-documented equivalent review; owner 'Matt Maitland' remains grant authority. |
| `offline_metric_gate_required` | `satisfied_by_upstream` | True | Generalization audit gates passed for the second surface with 528 candidate rows and material lift evidence before this request. |
| `production_observability_slo_required` | `satisfied_by_grant` | True | Grant records production-readiness observability SLO targets for run, component, error, latency, and write-target monitoring before any enablement. |
| `production_scope_rollback_disable_drill_required` | `satisfied_by_upstream` | True | Phase 2 disable drill passed with environment restoration; prior execution grant and policy provide rollback/disable templates for grant review. |
| `subgroup_or_slice_regression_review_required` | `satisfied_by_grant` | True | Grant scope remains emerging-family only and explicitly defers broader slices to the prod-scoped shadow pilot chain. |
| `user_visible_ranking_change_review_required` | `blocked_separate_chain` | True | This request explicitly excludes user-visible ranking changes; production-facing ranking requires separate authorization. |

## Authorization Request

- Decision: `requested`
- Requester: Matt Maitland
- Requested at: 2026-05-29T03:42:02Z
- Request notes: None
- Requested scope: `production_readiness_for_bounded_online_shadow_only`

## Authorization Grant

- Decision: `granted`
- Owner: Matt Maitland
- Granted at: 2026-05-29T04:04:47Z
- Review by: 2026-08-27
- Expiry date: 2026-08-27
- Second reviewer: None
- Owner equivalent review: Owner reviewed the accepted Phase 2 bundle, criteria artifact, indexed policy and execution grant references, and grant-time gate resolutions as an equivalent production-readiness authorization review.
- Grant notes: None
- Granted scope: `production_readiness_for_bounded_online_shadow_only`

## Grant Resolution Summary

- Production-readiness authorization blocker cleared for the paperwork chain only.
- Grant-time partial/open gates resolved into satisfied_by_grant with documented bounded scope.
- Leakage, offline metric, and rollback/disable evidence remain satisfied_by_upstream.
- Production default/API and user-visible ranking changes remain blocked separate chains.
- Prod-scoped shadow plan/proof/pilot still required before any enablement.

## Explicitly Not Included

- online_shadow_execution_enabled globally
- production_default_allowed
- api_web_changes_allowed
- user_visible_ranking_changed
- DB writes/DDL
- model refit, embedding generation, label ingest
- production default / API / fleet-wide flag enablement

## Production/API/Default Separation

- Production default allowed: False
- API/web changes allowed: False
- User-visible ranking changed: False
- Writes performed: False
- Runtime writes performed: False

## Recommended Next Stage

`begin_production_scoped_online_shadow_plan_v1`

## Caveats

- Bundle request milestone only; grants nothing.
- Phase 2 accepted evidence is necessary but not sufficient.
- Gate partial/open statuses are inputs to owner grant review, not failures of this commit.
- This bundle does not enable online shadow execution.
- This bundle does not authorize production default/API/user-visible ranking behavior.
- Bundle grant milestone only; does not run prod shadow or enable global shadow.
- Clears production-readiness authorization blocker for paperwork chain only.
- Prod-scoped shadow plan/proof/pilot still required before any enablement.
- Production default/API/user-visible ranking remain separate authorization chains.
