# ML Shadow Scorer Spec (ml-shadow-scorer-v1-spec)

## Executive Summary

This artifact specifies `ml-shadow-scorer-v1` after fresh hybrid validation gates passed. It is a spec only: it does not implement or execute shadow scoring, and it does not authorize production default changes.

- Spec ready for implementation: True
- Shadow scoring allowed: False
- Production default allowed: False
- Recommended next stage: `implement_ml_shadow_scorer_v1_disabled_by_default`

## Evidence Chain

- Confirmatory validation passed: True
- Primary material lift passed: True
- Ranking run: `rank-9f4b2a2084` / `emerging`
- Snapshot: `source-snapshot-fresh-hybrid-v1-20260518`
- Embedding version: `fresh-hybrid-text-embedding-v1`
- Candidate pool SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`

## Frozen Formula

- Scorer ID: `ml-shadow-scorer-v1`
- Formula ID: `hybrid_rank_mean_50_50`
- Formula: `score = 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)`
- No eval-label weight tuning, refit, or label-derived features are allowed.

## Learned Component Source

- audit_embedding_probability_work from frozen ml-offline-audit-embedding-scorer-v2 application, as used in fresh validation.
- Scorer version: `ml-offline-audit-embedding-scorer-v2`.
- Fit mode: `holdout_bound_train_only`.

## Rank Percentile Definition

- Higher raw score is better.
- Ties use average rank.
- If n == 1: `rank_pct = 1.0`.
- Otherwise: `rank_pct = 1.0 - ((average_rank - 1.0) / (n - 1.0))`.
- Scope is the full candidate pool for the scoring run; future shadow runs compute within their own full candidate pool.

## Allowed Inputs

- `canonical_openalex_work_id`
- `final_score`
- `audit_embedding_probability_work`
- `ranking_run_id`
- `family`
- `corpus_snapshot_version`
- `embedding_version`
- `title`
- `year`
- `source metadata for audit display only`

## Forbidden Inputs

- `relevance_label`
- `novelty_label`
- `bridge_like_label`
- `good_or_acceptable`
- `label_any_positive`
- `any derived label targets`
- `reviewer_notes`
- `row_id`
- `sample_reason`
- `review_pool_variant`
- `holdout assignment`
- `fresh validation labels`
- `any feature selected or tuned using labels`

## Execution Boundaries

- Shadow scorer must be disabled by default.
- Future implementation may only write isolated shadow/audit outputs.
- Existing production ranking, API behavior, bridge defaults, and public UI remain unchanged.
- No production default promotion is authorized by this spec.

## Observability

- component coverage counts
- missing learned probability count
- score distribution for final_score
- score distribution for audit_embedding_probability_work
- score distribution for hybrid shadow score
- top-k overlap with heuristic final_score
- rank displacement summary
- family-level counts
- shadow output completeness
- error counters if implemented online
- latency counters if implemented online

## Future Gates

- implementation matches this exact formula
- learned component uses frozen ml-offline-audit-embedding-scorer-v2 output or successor explicitly validated by a new gate
- no production/default config changed
- shadow writes isolated from production ranking
- full component coverage
- no label leakage
- monitoring fields emitted
- rollback/disable path documented
- production default remains blocked

## Not Shadow Execution / Not Production

- This spec does not authorize shadow execution.
- This spec does not authorize production default, API, web, or model deployment changes.
- The missing implementation blocker remains until a future disabled-by-default implementation artifact exists.

## Caveats

- Spec only; no shadow scoring executed.
- Spec does not authorize shadow execution.
- Spec does not authorize production default, API, web, or model deployment changes.
- Frozen formula uses the primary hybrid arm confirmed on the fresh 143-work denominator.
- Learned component must use frozen scorer output without refit.
- No eval-label weight tuning or label-derived features are allowed.
