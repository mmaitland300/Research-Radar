# Recommendation review summary

- **Input:** `C:\dev\Cursor Projects\Research-Radar\docs\audit\manual-review\undercited_rank-ee2ba6c816_top20.csv`
- **Generated (UTC):** 2026-04-28T17:25:25.937356+00:00
- **Rows:** 20
- **Complete labels:** True

## Provenance (observed distinct values)

- **ranking_run_id:** rank-ee2ba6c816
- **ranking_version:** bridge-v2-nm1-zero-corpusv2-r2-k12-elig-top50-cross040-20260428
- **corpus_snapshot_version:** source-snapshot-v2-candidate-plan-20260428
- **embedding_version:** v2-title-abstract-1536-cleantext-r1
- **cluster_version:** kmeans-l2-v2-cleantext-r1-k12
- **family:** undercited

## Label counts (human labels only)

### relevance_label
- `acceptable`: 6
- `good`: 13
- `irrelevant`: 0
- `miss`: 1

### novelty_label
- `not_useful`: 1
- `obvious`: 1
- `surprising`: 3
- `useful`: 15

### bridge_like_label
- `no`: 0
- `not_applicable`: 20
- `partial`: 0
- `yes`: 0

## Metrics

- **precision_at_k_good_only:** 0.65
- **precision_at_k_good_or_acceptable:** 0.95
- **bridge_like_yes_or_partial_share:** None
- **surprising_or_useful_share:** 0.9
