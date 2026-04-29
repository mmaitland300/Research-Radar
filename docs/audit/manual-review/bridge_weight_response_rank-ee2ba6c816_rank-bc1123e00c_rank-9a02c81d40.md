# Bridge weight response rollup

This artifact is **not** validation and does **not** justify changing defaults.

## Stack

- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **bridge_eligibility_mode:** `top50_cross_cluster_gte_0_40`

## Movement (eligible bridge top-k)

- **zero vs w005 Jaccard:** `0.666667`
- **w005 vs w010 Jaccard:** `1.0`
- **zero vs w010 Jaccard:** `0.666667`

## Distinctness (eligible bridge vs emerging)

- **by run:** `{'zero': 0.212121, 'w005': 0.212121, 'w010': 0.212121}`
- **trend (overlap semantics):** `stable`

## Quality evidence

- **w010 new unlabeled eligible vs w005:** `0`
- **w010 eligible label coverage complete:** `True`

## Non-bridge stability

- **emerging unchanged (all comparisons):** `True`
- **undercited unchanged (all comparisons):** `True`

## Decision

- **weight_response_controlled:** `True`
- **weight_response_saturated:** `True`
- **recommend_next_weight_increase:** `False`
- **ready_for_default:** `False`

### Recommendation

0.10 did not improve eligible top-20 membership over 0.05; stop increasing weight until broader labels or a different scoring objective justify it. 0.05 remains a plausible experimental bridge-weight arm, not a default.

## Caveats

- This is not validation of bridge ranking quality.
- Evidence is largely single-reviewer, small-n (including top-20 worksheets / delta rows).
- No user study or product-facing evaluation is implied.
- Do not change defaults based on this artifact alone.
- This does not claim ML ranking superiority over simpler baselines.
