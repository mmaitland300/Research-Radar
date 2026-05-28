# ml-shadow-scorer-v1 Online Shadow Phase 2 Isolated Audit Write-Mode Proof (ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1)

## Executive Summary

This artifact records the isolated audit file-tree write-mode proof. Runtime scoring was scoped to this process, and all persistent proof output was constrained to the gitignored shadow-runs tree.

- Proof executed: True
- Proof passed: True
- Runtime writes performed: False
- Isolated artifact files written: True
- Files written: 4
- Cleanup completed: True
- Phase 2 writes authorized: False
- Recommended next stage: `request_phase2_isolated_audit_write_authorization_v1`

## Files Written

- `manifest.json`: 1974 bytes, sha256 `b7f3c42a18697f169d433e2344162e559368b32bc13d6db17c58d7d21ce0457f`
- `shadow_rows.jsonl`: 355191 bytes, sha256 `2e7846c7520038f5f4f8847cba0cc512e57dad16b263caca559359061fb26256`
- `observability.json`: 2992 bytes, sha256 `20667665b9adcf489e038d4b0698ade8a1625e8dd5cd11df9ce54b337fd773ae`
- `write_counts.json`: 565 bytes, sha256 `47f25049f285980047921fa8e29288dee34057208dc17a21744cd124f49bde52`

## Write Counts

- `ranking_runs`: 0
- `paper_scores`: 0
- `embeddings`: 0
- `labels`: 0
- `scorer_artifacts`: 0
- `production_config`: 0
- `production_default_pins`: 0
- `api_visible_tables`: 0
- `isolated_audit_shadow_artifacts`: 4
- `isolated_audit_shadow_tables`: 0

## Cleanup

- Cleanup target: `C:\dev\Cursor Projects\Research-Radar\docs\audit\shadow-runs\ml-shadow-scorer-v1\phase2-proof\rank-83787b91ef-20260528T163750Z`
- Directory absent after cleanup: True

## Caveats

- Proof writes are isolated audit file-tree writes only; runtime itself still reports writes_performed false.
- Proof JSON is the committed gate artifact; the pilot file tree is local and gitignored.
- File hashes and counts are retained in this proof artifact even when cleanup removes the local pilot directory.
- This proof does not authorize Phase 2 write pilots, fleet-wide online shadowing, production default, API/web behavior, or production readiness.
- The feature flag is scoped to this process and restored after preflight, pilot, and postflight calls.
