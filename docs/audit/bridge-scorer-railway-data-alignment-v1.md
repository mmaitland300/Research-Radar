# Bridge Scorer Railway Data Alignment v1

Generated: `2026-06-07T21:37:23.091584Z`

## Decision

- alignment_passed: `true`
- recommended_next_stage: `fix_bridge_canary_env_and_web_bridge_pin_then_rerun_deployed_readiness`

The Railway database now contains the pinned Bridge scorer run `rank-5a7efa5ca3` and cluster version `shadow-gen-kmeans-k12-v1`. The deployed API no longer returns `404` for the pinned Bridge run, and the read-only Bridge serving helper can score all 528 Bridge candidates from Railway Postgres.

## What Was Written

- `clustering_runs`: 1 succeeded row for `shadow-gen-kmeans-k12-v1`
- `clusters`: 528 rows
- `ranking_runs`: 1 succeeded row for `rank-5a7efa5ca3`
- `paper_scores`: 1177 rows for `rank-5a7efa5ca3`
  - Bridge: 528 rows, 528 non-null `bridge_score`
  - Emerging: 528 rows
  - Undercited: 121 rows

No API code, web code, model weights, or serving flags were changed by this data operation.

## Verification

- local/Railway work identity match: `True`
- Railway embedding coverage: `528/528`
- Railway cluster assignment coverage: `528/528`
- local/Railway cluster rows match: `True`
- local/Railway paper_scores rows match: `True`
- serving helper candidate count: `528`
- serving helper returned count: `20`
- serving helper writes_performed: `False`

Serving helper top 5:

- `https://openalex.org/W4417471638`
- `https://openalex.org/W7126213550`
- `https://openalex.org/W7128741623`
- `https://openalex.org/W4415337516`
- `https://openalex.org/W4401445948`

## Deployed HTTP Checks

| Check | Status | Mode | Run | Count |
|---|---:|---|---|---:|
| Default Bridge | `200` | `materialized_heuristic` | `rank-5a7efa5ca3` | `20` |
| Wrong-run Bridge | `200` | `materialized_heuristic` | `rank-83787b91ef` | `20` |
| Pinned Bridge | `200` | `materialized_heuristic` | `rank-5a7efa5ca3` | `20` |
| Canary pinned Bridge | `200` | `materialized_heuristic` | `rank-5a7efa5ca3` | `20` |
| Emerging with product version | `200` | `bounded_ml_scorer` | `rank-83787b91ef` | `20` |
| Emerging without version | `200` | `materialized_heuristic` | `rank-5a7efa5ca3` | `20` |

## Important Boundary

The pinned-run data blocker is resolved, but deployed Bridge canary serving is not verified yet. The canary request still returned `materialized_heuristic`, not `bounded_bridge_ml_scorer`.

Also, because `rank-5a7efa5ca3` is now visible in Railway, unfiltered API calls can resolve to this newer diagnostic run. Emerging remains correct when callers pass `ranking_version=shadow-generalization-product-candidate-ranking-v1`, which is the expected Vercel page behavior. The next implementation should make Bridge pinning explicit without moving Emerging off its product run.

## Next Steps

1. Confirm Railway Bridge env includes `ML_BRIDGE_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST=bridge-deploy-readiness-v1` and the API image has redeployed from the Docker artifact-copy fix.
2. Add a web/API routing fix so Bridge can request `rank-5a7efa5ca3` while Emerging stays on `shadow-generalization-product-candidate-ranking-v1`.
3. Rerun deployed readiness. The next pass should require `bounded_bridge_ml_scorer` on the canary request before any public rollout.
