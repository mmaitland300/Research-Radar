# Bridge weight labeled outcome

This artifact is **not** validation and does **not** justify changing defaults.

## Stack

- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **bridge_eligibility_mode:** `top50_cross_cluster_gte_0_40`

## Label coverage

- **all_runs_complete:** `True`

## Per-run metrics (eligible bridge top-20)

### zero
- **coverage_complete:** `True`
- **good_or_acceptable_share:** `0.95`
- **bridge_like_yes_or_partial_share:** `0.95`
- **eligible_bridge_vs_emerging_jaccard:** `0.212121`
- **full_bridge_vs_emerging_jaccard:** `0.73913`

### w005
- **coverage_complete:** `True`
- **good_or_acceptable_share:** `1.0`
- **bridge_like_yes_or_partial_share:** `1.0`
- **eligible_bridge_vs_emerging_jaccard:** `0.212121`
- **full_bridge_vs_emerging_jaccard:** `0.73913`

### w010
- **coverage_complete:** `True`
- **good_or_acceptable_share:** `1.0`
- **bridge_like_yes_or_partial_share:** `1.0`
- **eligible_bridge_vs_emerging_jaccard:** `0.212121`
- **full_bridge_vs_emerging_jaccard:** `0.666667`

## Movement (eligible bridge top-20)

- **zero vs w005 Jaccard:** `0.666667`
- **w005 vs w010 Jaccard:** `1.0`
- **zero vs w010 Jaccard:** `0.666667`

## Decision

- **zero_quality_baseline_ready:** `True`
- **w005_quality_preserved:** `True`
- **w010_quality_preserved:** `True`
- **response_saturated:** `True`
- **recommend_w005_as_experimental_arm:** `True`
- **recommend_next_weight_increase:** `False`
- **ready_for_default:** `False`

## Interpretation

- 0.05 is a plausible experimental bridge-weight arm, not a default.
- 0.10 did not improve eligible top-20 membership over 0.05; stop increasing weight for this stack.
- This is single-reviewer, top-20 offline evidence, not validation.

## Caveats

- This is not validation of bridge ranking quality.
- Evidence is single-reviewer, top-20, offline audit material only.
- No user study or product-facing evaluation is implied.
- Do not change defaults based on this artifact alone.
- This does not claim ML ranking superiority over simpler baselines.
