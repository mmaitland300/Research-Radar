# ml-shadow-scorer-v1 Online Shadow Phase 2 Isolated Audit Write Authorization Grant (ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1)

## Executive Summary

This artifact grants one bounded non-production Phase 2 isolated audit write pilot. It does not run the pilot, create shadow-run files, enable the feature flag globally, authorize DB writes, or change production behavior.

- Decision: `granted`
- Owner: Matt Maitland
- Review by: 2026-08-27
- Expiry date: 2026-08-27
- Phase 2 write pilot authorized: True
- Phase 2 writes authorized: True
- Online shadow execution enabled: False
- Recommended next stage: `run_online_shadow_phase2_isolated_audit_write_pilot_v1`

## Authorized Pilot Scope

- Authorization scope: `bounded_non_prod_phase2_isolated_audit_write_pilot_only`
- Environment: non-prod pilot only
- Write target root: `docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/`
- Write target type: `isolated_audit_shadow_artifacts`
- Allowed files: `manifest.json`, `shadow_rows.jsonl`, `observability.json`, `write_counts.json`
- Feature flag: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED`

## Write Boundaries

- Allowed targets: `isolated_audit_shadow_artifacts`
- DB writes allowed: False
- DB DDL allowed: False
- Production/API/web changes allowed: False
- Production default changes allowed: False

## Rollback

- Disable switch: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED=off`
- Cleanup scope: delete/archive only <pilot_run_id> subdirectory
- Never delete phase2-proof root: True

## Caveats

- Grant artifact only; it does not run the pilot or write shadow files.
- The future pilot may write only isolated audit artifacts under docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/<pilot_run_id>/.
- phase2_writes_authorized true means bounded isolated audit file-tree writes only, not production write access.
- No DB writes or DDL are authorized.
- Online shadow execution remains globally disabled.
- Production/default/API/user-visible behavior remains unchanged.
- Production readiness remains separate.
