# Recommendation review summary

- **Input:** `C:\dev\Cursor Projects\Research-Radar\docs\audit\manual-review\bridge_rank-3904fec89d_top20_labeled.csv`
- **Generated (UTC):** 2026-04-27T06:36:11.877666+00:00
- **Rows:** 20
- **Complete labels:** True

## Provenance (observed distinct values)

- **ranking_run_id:** rank-3904fec89d
- **ranking_version:** bridge-v2-nm1-zero-r3-k6-20260424
- **corpus_snapshot_version:** source-snapshot-20260425-044015
- **embedding_version:** v1-title-abstract-1536-cleantext-r3
- **cluster_version:** kmeans-l2-v0-cleantext-r3-k6
- **family:** bridge

## Label counts (human labels only)

### relevance_label
- `acceptable`: 2
- `good`: 18
- `irrelevant`: 0
- `miss`: 0

### novelty_label
- `not_useful`: 0
- `obvious`: 0
- `surprising`: 6
- `useful`: 14

### bridge_like_label
- `no`: 0
- `not_applicable`: 0
- `partial`: 10
- `yes`: 10

## Metrics

- **precision_at_k_good_only:** 0.9
- **precision_at_k_good_or_acceptable:** 1.0
- **bridge_like_yes_or_partial_share:** 1.0
- **surprising_or_useful_share:** 1.0
