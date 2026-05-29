# ml-shadow-scorer-v1 Production-Scoped Shadow Bundle (online-shadow-production-scoped-v1)

## Executive Summary

This bundle records the bounded 528-work audit-artifact production-scoped shadow pilot while keeping live production source reads, global shadow enablement, production default, API/web, and user-visible behavior disabled.

- Bundle revision: 7
- Production-scoped plan defined: True
- Production-scoped proof passed: True
- Missing production-scoped shadow proof: False
- Pilot authorization requested: True
- Pilot authorization granted: True
- Pilot authorized: True
- Pilot harness executed: True
- Pilot harness reviewed: True
- Pilot harness accepted: True
- Production-scoped pilot executed: True
- Production-scoped pilot passed: True
- Online shadow execution enabled: False
- Recommended next stage: `review_production_scoped_online_shadow_pilot_v1`

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
- Pilot execution authorized: True
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

## Pilot Harness

- Pilot surface: `bounded_fixture_pilot_harness`
- Pilot run id: `rank-83787b91ef-harness-20260529T162506Z`
- Fixture row count: 3
- Live prod source reads performed: False
- Harness passed: True
- Pilot executed: True
- Pilot run directory: `docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/rank-83787b91ef-harness-20260529T162506Z/`

## Pilot Harness Files

| Path | Bytes | Rows | SHA-256 |
| --- | ---: | ---: | --- |
| `manifest.json` | 1074 | None | `75d2e52ab521c7c9e7998407b61167d91367953f87870cda4516a4e3a2fbc98b` |
| `shadow_rows.jsonl` | 1685 | 3 | `925ef4b1685f30cb777097ca8b787c8df9a6cea65c7e2467084485b56f96fe0f` |
| `observability.json` | 1774 | None | `2da9ab90eaba117b912b2bce972ef9329a33d6bd7c783c7e6b6605d054a69c44` |
| `write_counts.json` | 741 | None | `7c8697e6c06b9300f4cd4ab6b81deabae834da500457484483f3a8796af0d457` |

## Pilot Harness Review

- Decision: `accepted`
- Reviewer: Matt Maitland
- Reviewed at: 2026-05-29T18:43:57Z
- Review notes: None
- Harness accepted: True
- Failed review checks: None
- Pilot executed: True

## Pilot Harness Review Checks

- `runtime_drill_pilot_status_succeeded_test_only`: True
- `fixture_row_count_3`: True
- `runtime_drill_call_order`: True
- `environment_restored`: True
- `forbidden_write_counts_zero`: True
- `isolated_artifact_count_4`: True
- `expected_files_recorded`: True
- `runtime_writes_false`: True
- `live_prod_source_reads_false`: True
- `pilot_surface_bounded_fixture`: True
- `actual_pilot_executed_false`: True
- `production_api_user_visible_unchanged`: True
- `labels_not_used`: True
- `pass_fail_overall_passed`: True
- `pass_fail_failed_checks_empty`: True

## Pilot Harness Review Limitations

- not live production traffic
- no live prod source reads were reviewed
- no runtime rerun was performed
- no shadow-runs artifact reads or writes were performed
- actual production-scoped pilot remains unexecuted

## Production-Scoped Pilot Run

- Pilot surface: `bounded_read_only_audit_artifact_pilot`
- Pilot run id: `rank-83787b91ef-20260529T210000Z`
- Joined candidate count: 528
- Live prod source reads performed: False
- Pilot passed: True
- Pilot run directory: `docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/rank-83787b91ef-20260529T210000Z/`

## Production-Scoped Pilot Source Artifacts

| Role | Path | SHA-256 |
| --- | --- | --- |
| learned_probability_artifact | `docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json` | `92df47cf9f49b4391404d170775cdcae6b4615423f852e2e8198562fbca778af` |
| second_surface_generalization_audit | `docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json` | `335d06c3ceae65c1420e12fc64bf9d9b9e20c19bfb762858d2299218e5253c96` |
| bundle | `docs/audit/bundles/production-scoped-shadow-v1/bundle.json` | `1a7888b7ff46ab07fc91a65eed2d5087c34881f95cdeb8eeed7c4caee8322c1c` |

## Production-Scoped Pilot Files

| Path | Bytes | Rows | SHA-256 |
| --- | ---: | ---: | --- |
| `manifest.json` | 3167 | None | `4452d6cd018a84b04b504227d0253c185eb2f1f53508e2774dff065e3a22a6f9` |
| `shadow_rows.jsonl` | 406935 | 528 | `a8fe921a4b5901de0e5a6ac66299ac5efb29297ed17c12e795929b87e7958f98` |
| `observability.json` | 15631 | None | `dd739991032e01f8121482c2afa893c53a5e588d9aa7f7693ff0a451dab19853` |
| `write_counts.json` | 742 | None | `9d73f2f1232f11512e138d41ca78e6ae82fc25389c05da93ded1829486cfd26a` |

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

`review_production_scoped_online_shadow_pilot_v1`

## Caveats

- Bundle plan surface only; does not run runtime or shadow scoring.
- Bundle does not enable online shadow execution or change the global feature flag default.
- Bundle does not authorize production default/API/user-visible ranking behavior.
- Bundle does not write shadow-runs files, databases, embeddings, labels, or scorer artifacts.
- Frozen upstream bundles and legacy artifacts remain referenced by path and SHA only.
- Proof is a bounded fixture/dry-run; not a live prod run and does not call runtime.
- Proof clears the prod-scoped shadow proof blocker only.
- Pilot authorization, live execution authorization, flag enablement, and prod default/API/user-visible remain separate gates.
- This is a bounded fixture pilot harness, not live production traffic.
- No live prod source reads were performed.
- No production-scoped pilot execution is recorded.
- Global shadow remains disabled.
- Production default/API/user-visible behavior remains unchanged.
- Actual production-scoped pilot remains a separate milestone.
- Review covers bounded fixture pilot harness plumbing evidence only, not live production traffic.
- No runtime rerun or shadow-runs artifact read was performed by the review.
- The actual production-scoped pilot remains unexecuted and separately gated.
- Pilot uses approved frozen second-surface audit artifacts, not live production traffic.
- No live production DB/source reads were performed.
- Live read-only production shadow access remains a separate future authorization chain.
- Global online shadow enablement remains false.
- Production default/API/user-visible behavior remains unchanged.
