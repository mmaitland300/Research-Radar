# ML targeted gap review worksheet (emerging)

## Purpose

This worksheet lists **emerging-family** candidate rows from one **persisted ranking run** chosen to grow **contrastive offline audit labels**, especially **explicit negatives** for `good_or_acceptable` (relevance) and `surprising_or_useful` (novelty). It is **not** model training output.

**Target gap focus for this export:** `good_or_acceptable` (use label columns to capture both relevance and novelty; the gap name names the primary coverage hole in the v2 readiness matrix).

## Why emerging negatives are the current bottleneck

The v2 label dataset and readiness matrix show **emerging** slices with **few or no negative** derived targets for relevance and/or novelty on `rank-ee2ba6c816`, while bridge and undercited already carry more contrast. A meaningful offline learned baseline would stay premature until emerging has **miss / irrelevant** and **not_useful / obvious** (or `neither`) examples from the **same** `paper_scores` pool.

## Selection strategy

- **Exclude** papers already **fully** labeled for this `ranking_run_id` + `emerging` + `paper_id` in the audit slice of the provided label dataset (all three label columns non-empty).
- **Prefer** bottom-of-list ranks (tail), **low final_score**, **low topic_growth_score**, **low citation_velocity_score**, and rows with **sparse topic metadata** or **low semantic_score**, then **deterministic fallback** fill.
- **Reviewer guidance for `bridge_like_label`:** leave blank in the CSV until review; when filling, use `not_applicable` unless you are **intentionally** judging bridge-like behavior for this emerging row.

## Row count by sample_reason

| sample_reason | rows |
| --- | ---: |
| `emerging_bottom_rank_tail` | 10 |
| `low_citation_velocity` | 4 |
| `low_topic_growth` | 4 |
| `off_slice_topic_metadata` | 2 |
| `weak_emerging_signal` | 5 |

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **target_gap:** `good_or_acceptable`
- **ranking_version:** `bridge-v2-nm1-zero-corpusv2-r2-k12-elig-top50-cross040-20260428`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **label_dataset:** `docs/audit/ml-label-dataset-v2.json`
- **generated_at:** `2026-04-30T16:52:03Z`

## Caveats

- This worksheet is not validation of ranking quality.
- Rows are selected from a persisted ranking run to improve emerging-family contrastive label coverage.
- Labels must be filled manually; missing labels must not be inferred.
- Do not create train/dev/test splits from this worksheet until a later explicit split policy exists.

This worksheet supports **targeted contrastive audit labeling** only; do not treat it as ranking validation.
