# Bridge Scorer Deployment Readiness v1

Generated: `2026-06-07T20:12:30Z`

## Decision

- deployment_readiness_passed: `false`
- recommended_next_stage: `fix_bridge_scorer_deployment_readiness`
- reason: `live_gate_open_not_verified`

This is a readiness limitation, not a rollback recommendation. The configured
database and local API integration checks passed, but deployed HTTP checks could
not be performed because `RESEARCH_RADAR_API_BASE` was not set, and no live
Bridge environment variables were mutated.

## Git and Deploy Baseline

- local HEAD: `4a52abbc2f6f49c97d96f116473d2a5d649ee688`
- origin/main HEAD: `4a52abbc2f6f49c97d96f116473d2a5d649ee688`
- local HEAD matches origin/main: `true`
- `75d5f44` on origin/main: `true`
- deployed commit available: `false`
- deployed commit includes Bridge gate: `not verified`

`origin/main` includes `75d5f44`
(`feat(recommendations): add bounded Bridge scorer serving gate`).

## Configured DB Readiness

Checks used `DATABASE_URL`. `PROD_DATABASE_URL` was intentionally ignored. The
configured host was `localhost`, so this is recorded as production-like data
readiness evidence rather than independent proof of a remote production DB.

- ranking_run_id: `rank-5a7efa5ca3`
- ranking_version: `shadow-gen-bridge-score-diagnostic-v1`
- corpus_snapshot_version: `source-snapshot-shadow-generalization-v1-20260521`
- embedding_version: `shadow-generalization-text-embedding-v1`
- status: `succeeded`
- Bridge rows: `528`
- distinct Bridge work ids: `528`
- bridge_score coverage: `528/528`
- embedding coverage: `528/528`

The current default Bridge run resolved from the configured DB is
`rank-5a7efa5ca3`, which matches the pinned Bridge scorer run.

## Serving Helper Smoke

The real scorer helper was exercised with:

`rank_bridge_recommendations_with_scorer(database_url=DATABASE_URL, limit=20)`

Result:

- passed: `true`
- candidate_count: `528`
- scored_candidate_count: `528`
- returned_count: `20`
- alpha: `0.5`
- rank_pct_scope: `full_bridge_candidate_pool`
- scorer_probability_source: `full_pool_frozen_inference_not_oof`
- writes_performed: `false`

Top five scorer-served work ids:

- `W4417471638`
- `W7126213550`
- `W7128741623`
- `W4415337516`
- `W4401445948`

## Deployed HTTP Checks

Not performed. `RESEARCH_RADAR_API_BASE` was not set.

Not verified on deployed API:

- default Bridge fail-closed response
- mismatched run fail-closed response
- live gate-open response
- cap exhaustion
- Emerging and Undercited deployed behavior
- deployed commit identity

## Local API Integration Checks

FastAPI `TestClient` checks were run in-process against the configured
`DATABASE_URL` with process-local env vars only.

With no `ML_BRIDGE_SCORER_V1_*` env vars:

- Bridge `limit=20`: HTTP `200`, `ranking_mode=materialized_heuristic`
- Bridge `limit=19`: HTTP `200`, `ranking_mode=materialized_heuristic`
- Bridge `bridge_eligible_only=true`: HTTP `200`, `ranking_mode=materialized_heuristic`
- Bridge wrong run `rank-83787b91ef`: HTTP `200`, `ranking_mode=materialized_heuristic`

With controlled process-local Bridge env:

- `ML_BRIDGE_SCORER_V1_RUNTIME_ENABLED=true`
- `ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_ENABLED=true`
- `ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_PERCENT=100`
- `ML_BRIDGE_SCORER_V1_ROLLOUT_EXPOSURE_CAP=1`
- `ML_BRIDGE_SCORER_V1_RANKING_RUN_ID=rank-5a7efa5ca3`
- empty cohort allowlist

Pinned gate-open request:

- HTTP `200`
- `ranking_mode=bounded_bridge_ml_scorer`
- `scorer_surface=bridge`
- `bridge_recommendations_ml_served=true`
- `bridge_rank_pct_hybrid_alpha=0.5`
- `bridge_rank_pct_scope=full_bridge_candidate_pool`
- `emitted_to_public_users=true`
- item count: `20`

Failure cases with Bridge env:

- second request after cap `1`: `materialized_heuristic`
- `limit=19`: `materialized_heuristic`
- `bridge_eligible_only=true`: `materialized_heuristic`
- wrong run `rank-83787b91ef`: `materialized_heuristic`

Other family checks:

- Emerging remained `materialized_heuristic`
- Undercited remained `materialized_heuristic`
- `ML_SHADOW_SCORER_V1_*` env vars did not open Bridge

## Caveats

- No deployed API base URL was configured.
- No live deployed Bridge env vars were enabled.
- The gate-open result was verified only in a local process against the
  configured DB.
- The configured DB data checks passed, but the host was `localhost`.
- This artifact does not authorize public Bridge ML traffic.

## Next Step

Set `RESEARCH_RADAR_API_BASE` to the deployed API, confirm the deployed commit,
and perform a temporary cap-1 or cap-2 deployed gate-open check. If that passes
without server errors or writes, the next stage can move to
`enable_bridge_scorer_tiny_canary_v1`.
