# ml-shadow-scorer-v1 Online Shadow Phase Bundle (online-shadow-phase2-v1)

## Executive Summary

This bundle is the canonical forward-facing Phase 2 write-path status view. It references frozen legacy artifacts by path and SHA; it does not run a pilot, enable runtime execution, or change production behavior.

- Bundle revision: 2
- Phase 2 write pilot authorized: True
- Phase 2 writes authorized: True
- Online shadow execution enabled: False
- Recommended next stage: `review_online_shadow_phase2_isolated_audit_write_pilot_v1`

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
| phase2_write_mode_plan | `docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json` | `2e12d1e81b1e2eb13e13c271b36ab407091076976c4877caec8a4a7d9a1b1e42` |
| phase2_write_mode_proof | `docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json` | `9f4529992025c7b9621ec1c922ec38ee434e8aee3841e7fb038ada972c938d14` |
| phase2_write_authorization_request | `docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json` | `5702578360002cee65edd36ae123e804b8d6117871e7de7a2d8fcd647ec3c6e8` |
| phase2_write_authorization_grant | `docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json` | `531e1ee3866afdc3c30d83c6aed85def7494e125d856f8b399f423283a50c410` |
| phase1_no_write_pilot_review | `docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json` | `2c898a1207aaf769c43c49fe0b736404a8eb2b7f17766f39c28d5a976c25de18` |
| prior_execution_authorization_grant | `docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json` | `e9505ebad2033598c6b8f923a2cfc58f362154a1076a2e3014ebafa7d23525f8` |
| online_shadow_policy | `docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json` | `726b790d1539a7ea158c484c7c374ae3f002f3e0f8fffa4238d3c73fca30e378` |

## Authorization Rollup

- Decision: `granted`
- Owner: Matt Maitland
- Review by: 2026-08-27
- Expiry date: 2026-08-27
- Write pilot scope: `bounded_non_prod_phase2_isolated_audit_write_pilot_only`
- Write authorization scope: `isolated_audit_shadow_artifacts_only`

## Evidence Rollup

- Phase 1 no-write pilot accepted: True
- Phase 2 write-mode proof passed: True
- Proof summary reference: `phase2_write_authorization_grant.proof_summary`

## Execution And Review

- Phase 2 write pilot executed: True
- Phase 2 write pilot reviewed: False
- Phase 2 write pilot passed: True
- Pilot run id: `rank-83787b91ef-20260528T212715Z`
- Pilot run directory: `docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/rank-83787b91ef-20260528T212715Z/`
- Isolated files written: 4
- Forbidden write targets zero: True

### Pilot Files

- `manifest.json`: 1759 bytes, sha256 `92d15c6f09224c6a6b3ecaf24c3517f8e6c6d9de6a00fb603d2588f8def4735b`
- `shadow_rows.jsonl`: 355191 bytes, sha256 `2e7846c7520038f5f4f8847cba0cc512e57dad16b263caca559359061fb26256`
- `observability.json`: 2992 bytes, sha256 `dce84142bd4e8c5a02a50d5b3e2050c492ecb07da57bcf6995b698ed60e6e7cb`
- `write_counts.json`: 565 bytes, sha256 `4484153b6a6645ad8aa4944743940e3e392ab3c4bcaed2d1ee404b4338a16b18`

## Production/API/Default Separation

- Production default allowed: False
- API/web changes allowed: False
- User-visible ranking changed: False
- Production readiness authorization missing: True
- Phase 2 write pilot authorization missing: False

## Recommended Next Stage

`review_online_shadow_phase2_isolated_audit_write_pilot_v1`

## Caveats

- Bundle only; does not run the Phase 2 write pilot.
- Bundle does not enable online shadow execution.
- Bundle does not authorize production default/API/user-visible ranking behavior.
- Bundle does not authorize production readiness.
- Legacy artifacts remain frozen evidence and are referenced by path + SHA.
- Future pilot updates should modify the bundle execution section, not create new request/grant/proof artifact families.
