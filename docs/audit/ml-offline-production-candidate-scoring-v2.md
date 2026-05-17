# Production-Candidate Offline Scoring (ml-offline-production-candidate-scoring-v2)

## Executive Summary

Offline product-candidate diagnostic over an existing ranking run. No ranking was run, no product scores were written, and no model artifact was produced.

- **ranking_run_id:** `rank-ee2ba6c816`
- **family:** `emerging`
- **target:** `good_or_acceptable`
- **scoring_mode:** `heuristic_and_audit_embedding_scorer`
- **candidate rows:** 217
- **labeled eval works:** 217
- **heuristic ROC-AUC/AP:** 0.804 / 0.958
- **learned audit scorer ROC-AUC/AP:** 1.000 / 1.000
- **learned vs heuristic deltas (ROC-AUC/AP/P@10):** 0.196 / 0.042 / 0.000

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

## Learned Audit Scorer Metrics

- **Score aggregation policy:** `max_probability`

| Metric | Value | Note |
| --- | ---: | --- |
| ROC-AUC (Mann-Whitney) | 1.000 |  |
| Average precision | 1.000 |  |
| Precision@5 | 1.000 |  |
| Recall@5 | 0.026 |  |
| Precision@10 | 1.000 |  |
| Recall@10 | 0.053 |  |
| Precision@20 | 1.000 |  |
| Recall@20 | 0.105 |  |

## Heuristic vs Learned Comparison

| Metric | Heuristic final_score | Learned audit scorer | Delta learned-heuristic |
| --- | ---: | ---: | ---: |
| ROC-AUC | 0.804 | 1.000 | 0.196 |
| Average precision | 0.958 | 1.000 | 0.042 |
| Precision@10 | 1.000 | 1.000 | 0.000 |

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
- Frozen audit scorer trained on audit corpus only.
- Product pool application is offline diagnostic only.
- Product candidates were not used for training.
- Learned audit scorer and heuristic final_score are separate evidence lines.
- No shadow scoring or production default change.
- No ranking/API/web changes.

## Next Step

Product-candidate metric gates v2.
