# Bridge eligibility sensitivity

Read-only diagnostic artifact; this does **not** validate ranking quality and does **not** validate bridge behavior.
No bridge weights were changed.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **ranking_version:** `bridge-v2-nm1-zero-corpusv2-r2-k12-elig-top50-cross040-20260428`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **k:** `20`

## Baseline overlap

- **full_bridge_vs_emerging_jaccard:** `0.73913`
- **full_bridge_top_k_ids:** `[136, 19, 21, 15, 20, 104, 11, 14, 16, 9, 10, 12, 30, 125, 126, 131, 143, 138, 137, 110]`
- **emerging_top_k_ids:** `[136, 137, 19, 21, 104, 15, 138, 20, 11, 16, 14, 116, 30, 128, 9, 10, 18, 125, 12, 131]`

## Signal field coverage

- **field_present_count:** `{"anchor_cluster_id": 217, "cross_cluster_neighbor_share": 217, "distinct_neighbor_cluster_count": 217, "eligible": 217, "foreign_neighbor_count": 217, "mix_score": 217, "neighbor_work_ids": 217, "same_cluster_neighbor_share": 217, "signal_version": 217}`
- **field_mapping_used:** `{"cross_cluster_neighbor_share": "mix_score (fallback) or 1 - same_cluster_neighbor_share", "distinct_neighbor_cluster_count": "derived from neighbor_work_ids + clusters table", "same_cluster_neighbor_share": "derived from anchor_cluster_id + neighbor_work_ids + clusters table"}`

## Variant comparison

| variant_id | eligible_total | eligible_top_k | variant_vs_emerging_jaccard | delta_vs_full | distinctness_improves | candidate_for_zero_weight_rerun |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| existing_bridge_eligible | 93 | 20 | 0.212121 | 0.527009 | True | True |
| bridge_score_top_75pct | 163 | 20 | 0.428571 | 0.310559 | True | True |
| bridge_score_top_50pct | 108 | 20 | 0.25 | 0.48913 | True | True |
| bridge_score_top_25pct | 54 | 20 | 0.212121 | 0.527009 | True | True |
| distinct_neighbor_clusters_gte_2 | 209 | 20 | 0.73913 | 0.0 | False | False |
| distinct_neighbor_clusters_gte_3 | 195 | 20 | 0.73913 | 0.0 | False | False |
| same_cluster_share_lte_0_75 | 190 | 20 | 0.666667 | 0.072463 | False | False |
| same_cluster_share_lte_0_6 | 168 | 20 | 0.481481 | 0.257649 | True | True |
| same_cluster_share_lte_0_5 | 137 | 20 | 0.428571 | 0.310559 | True | True |
| cross_cluster_share_gte_0_25 | 190 | 20 | 0.666667 | 0.072463 | False | False |
| cross_cluster_share_gte_0_4 | 168 | 20 | 0.481481 | 0.257649 | True | True |
| cross_cluster_share_gte_0_5 | 137 | 20 | 0.428571 | 0.310559 | True | True |
| top50_and_cross_cluster_gte_0_40 | 92 | 20 | 0.212121 | 0.527009 | True | True |
| top50_and_same_cluster_lte_0_60 | 92 | 20 | 0.212121 | 0.527009 | True | True |
| top25_and_cross_cluster_gte_0_40 | 52 | 20 | 0.212121 | 0.527009 | True | True |

## Recommended next step

- `rerun_zero_bridge_with_candidate_threshold`

## Interpretation guardrail

- This artifact is **diagnostic, not validation**.
- The observed distinctness invariance across bridge weights indicates the fixed eligibility rule is controlling the eligible cohort.
- Under the current eligibility setting (`top50_cross_cluster_gte_0_40`), bridge weight should **not** be increased.

> Caveat: read-only diagnostic, not ranking validation, not bridge validation.
> Caveat: no bridge weights changed.
