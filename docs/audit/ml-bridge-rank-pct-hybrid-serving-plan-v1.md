# Bridge rank-percentile hybrid serving plan v1

Planning artifact for the next bounded Bridge serving-gate implementation. This file does not change production serving behavior.

## Plan Identity

- artifact_type: `ml_bridge_rank_pct_hybrid_serving_plan`
- plan_version: `ml-bridge-rank-pct-hybrid-serving-plan-v1`
- target_surface: `bridge`
- ranking_run_id: `rank-5a7efa5ca3`
- scorer: `bridge_recommendable_v3`
- selected_frozen_coefficient_C: `0.001`
- primary_alpha: `0.5`
- exploratory_alpha: `0.7`
- formula: `alpha * rank_pct(v3_ml_probability) + (1-alpha) * rank_pct(bridge_score)`
- rank_pct_formula: `1 - average_rank / n`
- rank_pct_scope: `full_bridge_candidate_pool`

## Validated Preconditions

| check | value |
| --- | --- |
| controlled_rollout_eval_ready | `True` |
| controlled rollout next stage | `draft_bridge_rank_pct_hybrid_serving_plan_v1` |
| rank-pct eval next stage | `authorize_bridge_hybrid_serving_controlled_rollout_eval` |
| linear hybrid guardrail next stage | `do_not_authorize_bridge_hybrid_serving_recheck_alpha_or_formula` |
| sensitivity ready | `True` |
| selected frozen scorer present | `True` |
| bridge_score coverage | `528/528` |
| primary alpha source | `primary_alpha_0_5_summary` |
| top20_quality_delta_labeled_only | `0.6` |

## Controlled Rollout Evidence

- Full top-20 churn: `20/20`
- Current top-20 labeled precision: `0.4`
- Proposed hybrid top-20 labeled precision: `1.0`
- Proposed hybrid top-20 labels: `13` labeled, `7` unlabeled
- Demoted labeled positives classified competitive: `8`

## Pinned Run Context

- ranking_run_id: `rank-5a7efa5ca3`
- ranking_version: `not_present_in_file_artifacts` (fail_closed_if_available)
- corpus_snapshot_version: `not_present_in_file_artifacts` (fail_closed_if_available)
- embedding_version: `shadow-generalization-text-embedding-v1`
- candidate_count: `528`
- bridge_score_coverage: `528/528`

Future serving must fail closed on `ranking_run_id` mismatch. If `ranking_version` or `corpus_snapshot_version` are available in later artifacts or API context, serving must also fail closed on those mismatches.

## Frozen Scorer Load Contract

- sensitivity artifact: `docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json`
- sensitivity SHA256: `04a41f91cee1a2a78b7f1f8e9f99b1ef13679a8dacd1b404a848c82751d807d2`
- embeddings provenance: `docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json`
- embeddings SHA256: `c6b3293753df76e9db7c6440fe6f48b5d6eb1ead84b2b46bb39fe83090a8cbf8`
- scorer_probability_source: `full_pool_frozen_inference_not_oof`
- Future serving uses full-pool frozen inference, not OOF probabilities.

## Serving Scope

- route: `/api/v1/recommendations/ranked`
- family: `bridge` only
- ranking_run_id: `rank-5a7efa5ca3` only, unless explicitly extended later
- limit: `20` only for scorer-served Bridge responses
- No Emerging changes, no Undercited changes, and no broad/fleet/default rollout.
- Load all Bridge candidates for the pinned run, recompute both rank-percentile inputs over the full pool, and return the top limit by hybrid rank.
- Hybrid reorder only: `final_score` and existing signal fields remain materialized metadata while `ranking_mode_detail` explains the rank-pct hybrid ordering.

## Bridge Gate Contract

- New Bridge-only gate module required: `apps/api/app/ml_bridge_scorer_rollout_gate.py`
- Use `ML_BRIDGE_SCORER_V1_*` environment variables.
- Do not extend `ML_SHADOW_SCORER_V1_*`, `ml_scorer_rollout_gate.py`, or `ml_scorer_rollout.py` for Bridge.
- Preserve the current Emerging gate behavior that blocks Bridge requests.

## API Response Contract

- `RankedRankingMode` future values: `['materialized_heuristic', 'bounded_ml_scorer', 'bounded_bridge_ml_scorer']`
- Bridge scorer responses use `bounded_bridge_ml_scorer`, not `bounded_ml_scorer`.
- Required response fields: `['ranking_mode', 'ranking_mode_detail', 'scorer_surface', 'bridge_recommendations_ml_served', 'bridge_rank_pct_hybrid_alpha', 'bridge_rank_pct_scope', 'emitted_to_public_users']`

## Web Display Contract

- Web copy must not call Bridge "validated."
- Allowed wording:
  - Bridge order selected by bounded ML scorer rollout
  - Experimental Bridge ranking
  - Blends bridge_score with a frozen Bridge ML scorer
  - Still under evaluation
- Ordering may change; materialized score/signal fields remain metadata.

## Failure And Fallback

- Fallback: `current_materialized_bridge_ranking`
- Fail closed when: Bridge env flag disabled
- Fail closed when: rollout cap reached
- Fail closed when: public rollout disabled
- Fail closed when: cohort/user not eligible
- Fail closed when: ranking_run_id mismatch
- Fail closed when: ranking_version mismatch, if available
- Fail closed when: corpus_snapshot_version mismatch, if available
- Fail closed when: family != bridge
- Fail closed when: limit != 20
- Fail closed when: bridge_score missing for any candidate needed in full-pool scoring
- Fail closed when: frozen scorer unavailable
- Fail closed when: embeddings unavailable
- Fail closed when: scoring raises
- Fail closed when: DB read fails

## Observability

- Log metadata only.
- Fields: `['ranking_mode', 'family', 'route', 'gate decision', 'reason closed', 'current_served', 'cap', 'ranking_run_id', 'public_rollout_enabled', 'public_rollout_percent']`
- Do not log: `['user IDs', 'full paper payloads']`

## Caveats

- Controlled rollout eval is offline only.
- Proposed top-20 has 7 unlabeled papers.
- Full top-20 churn means user-visible Bridge behavior would change substantially.
- There were 8 demoted labeled positives, all classified as competitive rather than clear losses.
- Single-reviewer labels only.
- No external validation.
- No multi-reviewer agreement.
- Serving plan does not itself authorize broad rollout.
- Alpha=0.7 is exploratory only; serving v1 should use alpha=0.5 unless a later artifact explicitly changes that.

## Next Stage

- **recommended_next_stage:** `implement_bridge_rank_pct_hybrid_serving_gate_v1`
- If preconditions fail: `collect_bridge_rollout_review_labels_before_serving`
