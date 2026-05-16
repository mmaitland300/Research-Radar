# Production-Candidate Offline Scoring (ml-offline-production-candidate-scoring-v1)

## Executive Summary

Offline product-candidate diagnostic over an existing ranking run. No ranking was run, no product scores were written, and no model artifact was produced.

- **ranking_run_id:** `rank-ee2ba6c816`
- **family:** `emerging`
- **target:** `good_or_acceptable`
- **scoring_mode:** `heuristic_and_coverage_only`
- **candidate rows:** 217
- **labeled eval works:** 217

## Candidate Pool Definition

Existing `paper_scores` rows filtered by explicit `ranking_run_id` and `recommendation_family`, ordered by persisted `final_score` descending. This command is SELECT-only and reuses the materialized pool.

## Label/Embedding Coverage

- **explicit target observations:** 427
- **joined labeled observations:** 286
- **joined labeled works:** 217
- **candidate work label coverage:** 1.000
- **unlabeled candidate works:** 0
- **candidate overlap rate by observation:** 0.670
- **embeddings present for joined observations:** 286
- **missing embeddings among joined observations:** 0

## Heuristic Final_Score Metrics

| Metric | Value | Note |
| --- | ---: | --- |
| ROC-AUC (Mann-Whitney) | 0.804 |  |
| Average precision | 0.958 |  |
| Precision@5 | 1.000 |  |
| Recall@5 | 0.026 |  |
| Precision@10 | 1.000 |  |
| Recall@10 | 0.053 |  |
| Precision@20 | 1.000 |  |
| Recall@20 | 0.105 |  |

## Learned/Embedding Metric Status

`heuristic_and_coverage_only`: learned product scores were not produced. ml-offline-ranker-experiment-v1 contains per-fold coefficients only; no frozen full-fit audit scorer export exists.

## Top-K Labeled Coverage

| k | Candidate works | Labeled works | Coverage | Labeled positives | Labeled negatives |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 5 | 5 | 1.000 | 5 | 0 |
| 10 | 10 | 10 | 1.000 | 10 | 0 |
| 20 | 20 | 20 | 1.000 | 20 | 0 |

## Not Shadow / Not Production

- This is not shadow scoring.
- This is not production scoring.
- Production defaults remain blocked.
- No `ml-shadow-scorer-v1` contract exists.
- No production model artifact exists.

## Caveats

- Not validation.
- Product-candidate offline diagnostic only.
- Existing ranking run reused read-only; no new ranking was run.
- Single-reviewer audit labels.
- Label coverage is incomplete and may bias metrics.
- No production model artifact.
- No shadow scoring or production default change.

## Next Step

Product-candidate metric gates v1 if results are credible; otherwise targeted product-pool labels.
