# ml-shadow-scorer-v1 Production-Scoped Shadow Bundle (online-shadow-production-scoped-v1)

## Executive Summary

This bundle records the production-scoped shadow pilot authorization grant while keeping pilot execution, runtime, production default, API/web, and user-visible behavior disabled.

- Bundle revision: 4
- Production-scoped plan defined: True
- Production-scoped proof passed: True
- Missing production-scoped shadow proof: False
- Pilot authorization requested: True
- Pilot authorization granted: True
- Pilot authorized: True
- Online shadow execution enabled: False
- Recommended next stage: `run_production_scoped_online_shadow_pilot_v1`

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
| production_readiness_bundle | `docs/audit/bundles/production-readiness-v1/bundle.json` | `ddaf3e10bebf48ccd5f920bada6256d72943cc3be147b5c82050cfae41ccfb00` |
| phase2_bundle | `docs/audit/bundles/phase2-v1/bundle.json` | `9b077aae115a161580110373a4df722ba4570657cd89d01f994174cc96fdce8d` |
| online_shadow_policy | `docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json` | `726b790d1539a7ea158c484c7c374ae3f002f3e0f8fffa4238d3c73fca30e378` |
| execution_authorization_grant | `docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json` | `e9505ebad2033598c6b8f923a2cfc58f362154a1076a2e3014ebafa7d23525f8` |
| phase2_write_mode_plan | `docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json` | `2e12d1e81b1e2eb13e13c271b36ab407091076976c4877caec8a4a7d9a1b1e42` |
| phase2_write_mode_proof | `docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json` | `9f4529992025c7b9621ec1c922ec38ee434e8aee3841e7fb038ada972c938d14` |
| generalization_audit_gates | `docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json` | `f76345f49d2077008e09fbc921b3c51e4483778422539e0936aead74691e2c84` |

## Upstream Evidence

- Production-readiness bundle: `docs/audit/bundles/production-readiness-v1/bundle.json`
- Production-readiness revision: 2
- Production-readiness authorization granted: True
- Phase 2 bundle: `docs/audit/bundles/phase2-v1/bundle.json`
- Phase 2 revision: 3
- Phase 2 write pilot accepted: True

## Plan Contract

- Decision: `planned`
- Planner: Matt Maitland
- Planned at: 2026-05-29T04:45:03Z
- Plan notes: None
- Future artifact root proposal: `docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/<pilot_run_id>/`
- Runtime feature flag: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED`
- Results use: audit/monitoring only

## Plan Sections

- `prod_scoped_identity_and_rollout_boundaries`
- `feature_flag_iam_config_requirements`
- `prod_read_only_input_contract`
- `production_default_api_user_visible_separation`
- `observability_and_slo_plan`
- `rollback_and_revocation_drill_plan`
- `proof_and_pilot_prerequisites`
- `ci_and_live_gate_requirements`

## Proof Evidence

- Decision: `proven`
- Prover: Matt Maitland
- Proven at: 2026-05-29T05:10:00Z
- Proof surface: `bounded_fixture_dry_run`
- Pilot run id: `rank-83787b91ef-20260529T051000Z`
- Local artifact root: `docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/rank-83787b91ef-20260529T051000Z/`
- Local artifact writes performed: True
- Production writes performed: False
- Forbidden write counts zero: True
- Observability complete: True
- Rollback flag-off verified: True
- Overall passed: True

## Proof Files

| Path | Bytes | Rows | SHA-256 |
| --- | ---: | ---: | --- |
| `manifest.json` | 880 | None | `c37cb86db8f3aae7bcada51e8ca28aeb9da22a3389650a830deff6f1e3504f09` |
| `fixture_rows.jsonl` | 1723 | 3 | `2699a8f42f1425bfd1a5ebf0f49cfbaed0f6a6df89543c871091e49ebdea7579` |
| `observability.json` | 1387 | None | `7431a9db63a363d9c3583f970eb4e19fee85fb9232c7e0542ae3b82d8ddc9fcb` |
| `write_counts.json` | 557 | None | `792edebcb0dc9e51c35ecb1fc4461e63da285e4ee96154282ea1385b70952095` |

## Authorization Boundaries

- Plan authorization scope: `production_scoped_shadow_plan_paperwork_only`
- Proof allowed by plan: True
- Pilot authorization requested: True
- Live execution authorized: False
- Execution authorized: False
- Proof authorized: False
- Pilot authorized: True

## Pilot Authorization Request

- Decision: `requested`
- Requester: Matt Maitland
- Requested at: 2026-05-29T15:08:18Z
- Request notes: None
- Requested scope: `production_scoped_shadow_pilot_paperwork_only`
- Missing pilot authorization: False

## Pilot Authorization Grant

- Decision: `granted`
- Owner: Matt Maitland
- Granted at: 2026-05-29T15:38:46Z
- Expiry date: 2026-08-27
- Review by: 2026-08-27
- Grant notes: None
- Second reviewer: None
- Owner equivalent review: Owner reviewed the production-scoped proof, pilot request, and bounded pilot contract as equivalent authorization review.
- Granted scope: `production_scoped_shadow_pilot_authorization_only`
- Missing pilot authorization: False

## Explicitly Not Included

- API/web
- DB writes/DDL
- api_web_changes_allowed
- fleet-wide flag enablement
- global flag enablement
- live prod execution beyond an explicitly granted bounded pilot
- model refit, embedding generation, label ingest
- online_shadow_execution_enabled globally
- prod default
- production default / API / fleet-wide enablement
- production_default_allowed
- user-visible ranking
- user-visible ranking changes
- user_visible_ranking_changed

## Production/API/Default Separation

- Production default allowed: False
- API/web changes allowed: False
- User-visible ranking changed: False
- Writes performed: False
- Runtime writes performed: False

## Recommended Next Stage

`run_production_scoped_online_shadow_pilot_v1`

## Caveats

- Bundle plan surface only; does not run runtime or shadow scoring.
- Bundle does not enable online shadow execution or change the global feature flag default.
- Bundle does not authorize production default/API/user-visible ranking behavior.
- Bundle does not write shadow-runs files, databases, embeddings, labels, or scorer artifacts.
- Frozen upstream bundles and legacy artifacts remain referenced by path and SHA only.
- Proof is a bounded fixture/dry-run; not a live prod run and does not call runtime.
- Proof clears the prod-scoped shadow proof blocker only.
- Pilot authorization, live execution authorization, flag enablement, and prod default/API/user-visible remain separate gates.
- Bundle pilot-grant milestone only; does not run the prod-scoped pilot.
- Clears prod-scoped pilot authorization blocker for the pilot chain only.
- Bounded pilot run still required before any enablement or prod default/API/user-visible change.
- Global shadow flag default remains off; prod default/API/user-visible remain separate chains.
