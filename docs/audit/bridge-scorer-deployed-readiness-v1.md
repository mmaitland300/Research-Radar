# Bridge Scorer Deployed Readiness v1

Generated: `2026-06-07T20:44:24Z`

## Decision

- deployed_readiness_passed: `false`
- recommended_next_stage: `fix_bridge_scorer_deployed_readiness`
- reason: `pinned_bridge_run_missing_in_deployed_database`

The deployed API is healthy and Bridge defaults remain fail-closed, but deployed
Bridge scorer readiness did not pass. The deployed API database resolves the
default Bridge run to `rank-83787b91ef` and returns `404` for the pinned scorer
run `rank-5a7efa5ca3`, so the Bridge scorer cannot open in the deployed
environment yet.

## Deployed API

- API base used: `https://capable-light-production.up.railway.app`
- API base source: prior Railway audit documentation, because
  `RESEARCH_RADAR_API_BASE` was not set.
- `/readyz`: HTTP `200`, database connected.
- `/health`: HTTP `200`.

## Commit and Deploy Identity

- local HEAD at verification start: `665be047fdd6501c8c7dc1c45f1497255ecb3786`
- origin/main HEAD at verification start: `665be047fdd6501c8c7dc1c45f1497255ecb3786`
- origin/main includes `75d5f44`: `true`
- GitHub Railway deployment id: `4967525867`
- GitHub Railway deployment SHA: `665be047fdd6501c8c7dc1c45f1497255ecb3786`
- GitHub Railway deployment state: `in_progress`
- deployed_commit_includes_bridge_gate: `true`

The deployed API response schema includes the Bridge scorer fields introduced by
`75d5f44`, but Railway deployment completion was not confirmed because the
GitHub deployment status remained `in_progress`.

## Deployed Fail-Closed Checks

Default Bridge request:

`GET /api/v1/recommendations/ranked?family=bridge&limit=20`

- HTTP `200`
- `ranking_mode=materialized_heuristic`
- `ranking_run_id=rank-83787b91ef`
- `bridge_recommendations_ml_served=null`
- item count: `20`

Wrong-run request:

`GET /api/v1/recommendations/ranked?family=bridge&limit=20&ranking_run_id=rank-83787b91ef`

- HTTP `200`
- `ranking_mode=materialized_heuristic`
- `bridge_recommendations_ml_served=null`
- item count: `20`

These pass the deployed fail-closed checks.

## Pinned Run Blocker

Pinned scorer run by id:

`GET /api/v1/recommendations/ranked?family=bridge&limit=20&ranking_run_id=rank-5a7efa5ca3`

- HTTP `404`

Pinned scorer run by version:

`GET /api/v1/recommendations/ranked?family=bridge&limit=20&ranking_version=shadow-gen-bridge-score-diagnostic-v1`

- HTTP `404`

This means Railway's deployed `DATABASE_URL` does not currently expose the
pinned Bridge scorer run. Gate-open would fail closed even if the Bridge env
vars were set correctly.

## Gate-Open Check

Not verified.

Temporary Railway env mutation was not performed because Railway CLI/token was
not available in this shell. Also, the deployed DB is missing
`rank-5a7efa5ca3`, so the canary gate-open check is blocked until deployed data
is aligned.

Top five gate-open paper ids: none recorded.

## Failure Cases

The full temporary-env failure matrix was not verified because the canary env
was not applied and pinned-run requests returned `404`.

Checks attempted under current deployed env:

- pinned request without cohort header: HTTP `404`
- pinned request with canary header: HTTP `404`
- pinned request with `limit=19`: HTTP `404`
- pinned request with `bridge_eligible_only=true`: HTTP `404`
- cap exhaustion: not verified

## Emerging and Undercited

Emerging deployed request:

- HTTP `200`
- `ranking_mode=bounded_ml_scorer`
- `ranking_run_id=rank-83787b91ef`
- item count: `20`

Undercited deployed request:

- HTTP `200`
- `ranking_mode=materialized_heuristic`
- `ranking_run_id=rank-83787b91ef`
- item count: `20`

Bridge env was not temporarily enabled, so before/after comparison under Bridge
env was not available.

## Packaging Fix

A deploy packaging bug was confirmed statically. `apps/api/Dockerfile` copied
only the Emerging scorer artifact:

- `docs/audit/ml-offline-audit-embedding-scorer-v2.json`

Bridge gate-open also needs these files inside the API image:

- `docs/audit/ml-bridge-rank-pct-hybrid-serving-plan-v1.json`
- `docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json`
- `docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json`

The Dockerfile was updated to copy those three artifacts. A local image build
passed, and a container check confirmed all three files exist under
`/app/docs/audit`.

This fix still needs to be pushed and redeployed before it can help the deployed
API.

## Limitations

- `RESEARCH_RADAR_API_BASE` was not set; the API base was taken from prior audit
  docs.
- Railway CLI/token was unavailable, so temporary env vars were not set or torn
  down.
- Railway deployment completion for `665be04` was not confirmed.
- The deployed DB is missing `rank-5a7efa5ca3`.
- Operator-side remote DB proof was unavailable because `DATABASE_URL` pointed
  at localhost.
- The Dockerfile packaging fix was verified locally, not on Railway.

## Next Fix

1. Push and redeploy the Dockerfile artifact-copy fix.
2. Align Railway's deployed database so `rank-5a7efa5ca3` exists with 528 Bridge
   rows, full `bridge_score` coverage, and full frozen-scorer embedding coverage.
3. Set a cohort-only Bridge env canary with cap `1` or `2`; do not set public
   rollout percent to `100` as first enablement.
4. Re-run deployed gate-open and failure-case checks.
