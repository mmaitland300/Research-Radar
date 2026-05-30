# ml-shadow-scorer-v1 Production-Scoped Shadow Bundle (online-shadow-production-scoped-v1)

## Executive Summary

This bundle records the bounded production-scoped online shadow live execution pilot run while keeping global shadow enablement, production default, API/web, and user-visible behavior disabled.

- Bundle revision: 15
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
- Production-scoped pilot reviewed: True
- Production-scoped pilot accepted: True
- Live read-only authorization requested: True
- Live read-only authorization granted: True
- Live read-only authorized: True
- Live read-only pilot executed: True
- Live read-only pilot passed: True
- Live read-only pilot reviewed: True
- Live read-only pilot accepted: True
- Live execution authorization requested: True
- Live execution authorization granted: True
- Live execution authorized: True
- Live execution pilot executed: True
- Live execution pilot passed: True
- Missing live execution authorization: False
- Live production source reads performed: True
- Online shadow execution enabled: False
- Recommended next stage: `review_production_scoped_online_shadow_live_execution_pilot_v1`

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
- Live execution authorized: True
- Execution authorized: False
- Pilot execution authorized: True
- Live read-only authorization requested: True
- Live read-only authorization granted: True
- Live read-only authorized: True
- Live execution authorization requested: True
- Live execution authorized: True
- Proof authorized: False
- Pilot authorized: True

## Live Read-Only Authorization Request

- Decision: `requested`
- Requester: Matt Maitland
- Requested at: 2026-05-29T21:24:02Z
- Request notes: Request production-scoped online shadow live read-only authorization after accepted bounded audit-artifact pilot review.
- Requested scope: `production_scoped_shadow_live_read_only_paperwork_only`
- Missing live read-only authorization: False
- Live reads performed: True

## Live Read-Only Future Grant Requirements

- exact IAM/read-only scope
- approved production source allowlist
- pinned identity and ranking-run/family boundaries
- incomplete coverage behavior must skip the whole run
- forbidden write targets remain zero
- observability requirements for live reads
- rollback/flag-off drill against live read mode
- no production default/API/user-visible behavior changes
- no global/fleet enablement
- time-bound grant/review requirements
- owner/second-review requirements

## Live Read-Only Request Explicitly Not Included

- global enablement
- production default
- API/web
- user-visible ranking
- DB writes/DDL
- refit/training
- fleet-wide enablement
- live reads at request time

## Live Execution Authorization Request

- Decision: `requested`
- Requester: Matt Maitland
- Requested at: 2026-05-30T16:17:22Z
- Request notes: Production-scoped live execution authorization request paperwork only.
- Requested scope: `production_scoped_shadow_live_execution_paperwork_only`
- Missing live execution authorization: False
- Live reads performed: True

## Live Execution Future Grant Requirements

- accepted live read-only pilot review evidence
- exact bounded live-execution scope under plan/proof contract
- pinned identity/ranking-run/family boundaries
- process-scoped runtime flag boundaries
- forbidden write targets remain zero
- rollback/flag-off drill for live execution mode
- observability requirements for live execution
- no production default/API/user-visible behavior changes
- no global/fleet enablement
- time-bound grant/review requirements
- owner/second-review requirements

## Live Execution Request Explicitly Not Included

- global enablement
- production default
- API/web
- user-visible ranking
- DB writes/DDL
- refit/training
- fleet-wide enablement
- live execution at request time
- online_shadow_execution_enabled globally
- production_default_allowed
- api_web_changes_allowed
- user_visible_ranking_changed

## Live Execution Authorization Grant

- Decision: `granted`
- Owner: Matt Maitland
- Granted at: 2026-05-30T17:10:33Z
- Expiry date: 2026-08-27
- Review by: 2026-08-27
- Grant notes: None
- Second reviewer: None
- Owner equivalent review: owner equivalent live execution grant review
- Granted scope: `production_scoped_shadow_live_execution_authorization_only`
- Missing live execution authorization: False
- Live reads performed: True

## Live Execution Grant Boundaries

- no bounded live execution pilot run performed at grant time
- no new live reads performed at grant time
- no writes performed at grant time
- no execution.live_execution_pilot_run slice recorded at grant time
- rev 15 run must prove bounded scope, flag-off behavior, and no forbidden writes

## Live Execution Grant Explicitly Not Included

- global enablement
- global/fleet online shadow execution
- production default
- API/web
- user-visible ranking changes
- production default/API/user-visible recommendation output
- DB writes/DDL
- refit/training
- fleet-wide enablement

## Live Execution Pilot Run

- Pilot surface: `bounded_live_execution_prod_scoped_pilot`
- Pilot run id: `prod-live-exec-rank-83787b91ef-20260530T175759Z`
- Joined candidate count: 528
- Live prod source reads performed: True
- Pilot passed: True
- Pilot run directory: `docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/prod-live-exec-rank-83787b91ef-20260530T175759Z/`
- Incomplete coverage status: `skipped_incomplete_coverage`
- Incomplete coverage shadow rows: 0

## Live Execution Source Reads

- Approved tables: ranking_runs, paper_scores, works, embeddings
- Ranking runs: 1
- Paper scores: 528
- Works: 528
- Embeddings: 528
- Candidate SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`

## Live Execution Pilot Checks

- `live_execution_grant_present`: True
- `joined_candidate_count_528`: True
- `runtime_row_count_528`: True
- `runtime_drill_call_order`: True
- `preflight_postflight_disabled`: True
- `pilot_status_succeeded_test_only`: True
- `process_scoped_runtime_flag_only`: True
- `environment_restored`: True
- `incomplete_coverage_skip_verified`: True
- `forbidden_write_counts_zero`: True
- `isolated_artifact_count_4`: True
- `expected_files_recorded`: True
- `production_api_user_visible_unchanged`: True
- `no_labels_refit_embedding_generation_or_label_ingest`: True
- `global_execution_authorization_false`: True

## Live Execution Pilot Files

| Path | Bytes | Rows | SHA-256 |
| --- | ---: | ---: | --- |
| `manifest.json` | 8430 | None | `0692a504f14bc8e625a0d9c901ecf37200c2483e55faead168ca88ab3081e149` |
| `shadow_rows.jsonl` | 407463 | 528 | `6d4464ae9be5dc46f330c69f867666dc664d62464562aa4b056bbfb597733e84` |
| `observability.json` | 15996 | None | `1988cd856a29ab39085531e7c3517e1e52c105cbbac2db1431bdcdf5327dc01d` |
| `write_counts.json` | 714 | None | `b7605a31930d226a8371422e0a7034cc3f9341082dddde20a9da5211331d6750` |

## Live Read-Only Authorization Grant

- Decision: `granted`
- Owner: Matt Maitland
- Granted at: 2026-05-29T22:16:01Z
- Expiry date: 2026-08-27
- Review by: 2026-08-27
- Grant notes: Record production-scoped live read-only authorization grant paperwork; no live reads performed at grant time.
- Second reviewer: None
- Owner equivalent review: Owner reviewed the production-scoped live read-only request, bounded pilot review, and grant contract as equivalent authorization review.
- Granted scope: `production_scoped_shadow_live_read_only_authorization_only`
- Missing live read-only authorization: False
- Live reads performed: True

## Live Read-Only Grant Boundaries

- read-only IAM scope
- approved source allowlist
- pinned identity/ranking-run/family boundaries
- skip-whole-run incomplete coverage
- process-scoped runtime flag
- zero forbidden writes
- rollback/flag-off drill

## Live Read-Only Grant Explicitly Not Included

- global enablement
- production default
- API/web
- user-visible ranking
- DB writes/DDL
- refit/training
- fleet-wide enablement
- live reads at grant time

## Live Read-Only Pilot Run

- Pilot surface: `bounded_live_read_only_prod_scoped_pilot`
- Pilot run id: `prod-readonly-rank-83787b91ef-20260530T032631Z`
- Joined candidate count: 528
- Live prod source reads performed: True
- Pilot passed: True
- Pilot run directory: `docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/prod-readonly-rank-83787b91ef-20260530T032631Z/`
- Labels used for scoring: False
- Refit/training performed: False
- Embedding generation performed: False
- Label ingest performed: False

## Live Read-Only Source Reads

- Approved tables: ranking_runs, paper_scores, works, embeddings
- Ranking runs: 1
- Paper scores: 528
- Works: 528
- Embeddings: 528
- Candidate SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`

## Live Read-Only Pilot Files

| Path | Bytes | Rows | SHA-256 |
| --- | ---: | ---: | --- |
| `manifest.json` | 4811 | None | `7a3e92a8f5fe9d7953c8563218c12e6741fa5baeaf87147518f63ac3e95da3f7` |
| `shadow_rows.jsonl` | 407463 | 528 | `7f702bb1c43eca86d6a7ec8e472c43ff5da315d9df8e71b2e5d925c9b4829d03` |
| `observability.json` | 15663 | None | `385f5504983f45889239fce8967b69d4aa967c45f25ae0745134a0463438cef0` |
| `write_counts.json` | 714 | None | `1901ab431a7426bbe2f458bf85262e5a5e5d50dcdf4a5e6ed452bc93ef4baf39` |

## Live Read-Only Pilot Review

- Decision: `accepted`
- Reviewer: Matt Maitland
- Reviewed at: 2026-05-30T04:17:01Z
- Review notes: Reviewed recorded production-scoped live read-only pilot evidence; accepted for live execution authorization request preparation.
- Live read-only pilot accepted: True
- Failed review checks: None
- Next stage: `review_production_scoped_online_shadow_live_execution_pilot_v1`

## Live Read-Only Pilot Review Checks

- `live_read_only_pilot_run_pass_fail_overall_passed`: True
- `joined_candidate_count_528`: True
- `runtime_row_count_528`: True
- `runtime_drill_call_order`: True
- `preflight_postflight_disabled`: True
- `environment_restored`: True
- `forbidden_write_counts_zero`: True
- `isolated_artifact_count_4`: True
- `expected_files_recorded`: True
- `live_prod_source_reads_true`: True
- `live_source_reads_documented`: True
- `no_labels_refit_embedding_or_label_ingest`: True
- `pilot_surface_bounded_live_read_only_prod_scoped`: True
- `harness_and_audit_pilot_still_no_live_reads`: True
- `production_api_user_visible_unchanged`: True
- `global_live_execution_authorization_false`: True
- `live_read_only_grant_slices_present`: True

## Live Read-Only Pilot Review Limitations

- live read-only pilot review evaluates recorded rev 11 evidence only
- no runtime rerun was performed
- no database connection was opened by the review
- no shadow-runs artifact reads or writes were performed
- global/live/fleet online shadow execution remains unauthorized
- accepted review clears only the live read-only pilot evidence gate

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
| bundle | `docs/audit/bundles/production-scoped-shadow-v1/bundle.json` | `1a7888b7ff46ab07fc91a65eed2d5087c34881f95cdeb8eeed7c4caee8322c1c` |
| learned_probability_artifact | `docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json` | `92df47cf9f49b4391404d170775cdcae6b4615423f852e2e8198562fbca778af` |
| second_surface_generalization_audit | `docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json` | `335d06c3ceae65c1420e12fc64bf9d9b9e20c19bfb762858d2299218e5253c96` |

## Production-Scoped Pilot Files

| Path | Bytes | Rows | SHA-256 |
| --- | ---: | ---: | --- |
| `manifest.json` | 3167 | None | `4452d6cd018a84b04b504227d0253c185eb2f1f53508e2774dff065e3a22a6f9` |
| `shadow_rows.jsonl` | 406935 | 528 | `a8fe921a4b5901de0e5a6ac66299ac5efb29297ed17c12e795929b87e7958f98` |
| `observability.json` | 15631 | None | `dd739991032e01f8121482c2afa893c53a5e588d9aa7f7693ff0a451dab19853` |
| `write_counts.json` | 742 | None | `9d73f2f1232f11512e138d41ca78e6ae82fc25389c05da93ded1829486cfd26a` |

## Production-Scoped Pilot Review

- Decision: `accepted`
- Reviewer: Matt Maitland
- Reviewed at: 2026-05-29T20:35:40Z
- Review notes: Reviewed bounded 528-work audit-artifact pilot evidence; accepted for live read-only authorization request preparation.
- Pilot accepted: True
- Failed review checks: None
- Next stage: `review_production_scoped_online_shadow_live_execution_pilot_v1`

## Production-Scoped Pilot Review Checks

- `pilot_run_pass_fail_overall_passed`: True
- `joined_candidate_count_528`: True
- `runtime_row_count_528`: True
- `runtime_drill_call_order`: True
- `preflight_postflight_disabled`: True
- `environment_restored`: True
- `forbidden_write_counts_zero`: True
- `isolated_artifact_count_4`: True
- `expected_files_recorded`: True
- `source_artifacts_verified`: True
- `runtime_writes_false`: True
- `live_prod_source_reads_false`: True
- `pilot_surface_bounded_read_only_audit_artifact`: True
- `production_api_user_visible_unchanged`: True
- `global_live_execution_authorization_false`: True

## Production-Scoped Pilot Review Limitations

- not live production traffic
- no live read-only production source access was reviewed
- no runtime rerun was performed
- no shadow-runs artifact reads or writes were performed
- global/live/fleet online shadow execution remains unauthorized

## Explicitly Not Included

- API/web
- DB writes/DDL
- api_web_changes_allowed
- fleet-wide enablement
- fleet-wide flag enablement
- global enablement
- global flag enablement
- global/fleet online shadow execution
- live execution at request time
- live prod execution beyond an explicitly granted bounded pilot
- live reads at grant time
- live reads at request time
- model refit, embedding generation, label ingest
- online_shadow_execution_enabled globally
- prod default
- production default
- production default / API / fleet-wide enablement
- production default/API/user-visible recommendation output
- production_default_allowed
- refit/training
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

`review_production_scoped_online_shadow_live_execution_pilot_v1`

## Caveats

- Bundle plan surface only; does not run runtime or shadow scoring.
- Bundle does not enable online shadow execution or change the global feature flag default.
- Bundle does not authorize production default/API/user-visible ranking behavior.
- Bundle does not write shadow-runs files, databases, embeddings, labels, or scorer artifacts.
- Frozen upstream bundles and legacy artifacts remain referenced by path and SHA only.
- Proof is a bounded fixture/dry-run; not a live prod run and does not call runtime.
- Proof clears the prod-scoped shadow proof blocker only.
- Pilot authorization, live execution authorization, flag enablement, and prod default/API/user-visible remain separate gates.
- Harness evidence remains bounded fixture-only and records no live source reads.
- Audit-artifact pilot evidence remains frozen-artifact-only and records no live production DB/source reads.
- Pilot and harness reviews did not rerun runtime or read shadow-runs artifacts.
- Live read-only pilot run evidence records live prod source reads under grant boundaries.
- Live read-only pilot run evidence records no production writes, committed artifact writes, or runtime writes.
- Live read-only pilot review evaluates recorded rev 11 evidence only.
- Review does not rerun runtime, open DB connections, or read shadow-runs files.
- Review does not grant global/live/fleet online shadow execution.
- Review does not change production default, API/web, or user-visible ranking.
- Accepted review clears the live read-only pilot evidence gate only.
- Bundle live-execution request milestone only; grants no live execution authorization.
- Accepted live read-only pilot review is necessary but not sufficient for live execution authorization.
- Live execution remains unauthorized until a separate grant milestone.
- Does not enable global/live/fleet online shadow execution.
- Production default/API/user-visible behavior remains unchanged.
- Live prod source reads already recorded by rev 11 remain unchanged; this request does not perform new reads.
- Grant milestone only; does not run bounded live execution pilot.
- Does not enable global/live/fleet online shadow execution.
- Does not change production default, API/web, or user-visible ranking.
- Does not perform new live reads at grant time.
- Bounded live execution pilot run remains a separate rev 15 milestone.
- Bounded live execution pilot run only; does not enable global/fleet online shadow execution.
- Does not change production default, API/web, or user-visible ranking.
- Runtime flag is enabled only inside the bounded pilot drill and restored afterward.
- Forbidden production write targets remain zero.
- Review is required before any further enablement chain.
