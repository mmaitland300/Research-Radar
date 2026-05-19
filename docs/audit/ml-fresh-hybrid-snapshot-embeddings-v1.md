# Fresh Hybrid Snapshot Embeddings (ml-fresh-hybrid-snapshot-embeddings-v1)

## Executive Summary

This artifact records title+abstract embedding generation for the eval-only fresh hybrid snapshot. It does not cluster, rank, write paper_scores, score hybrids, import labels, or authorize shadow/production.

- **Snapshot version:** `source-snapshot-fresh-hybrid-v1-20260518`
- **Embedding version:** `fresh-hybrid-text-embedding-v1`
- **Status:** `succeeded`
- **Mock embeddings:** False
- **Dry run:** False
- **Works considered:** 358
- **Embeddings written / skipped / failed:** 358 / 0 / 0
- **Full snapshot embedding coverage:** True
- **Recommended next stage:** `run_fresh_hybrid_product_candidate_ranking_v1`

## Model And Dimensions

- Provider: `openai`
- Model: `text-embedding-3-small`
- Dimensions: 1536
- Text source: `title_abstract`

## Coverage

- Snapshot work count: 358
- Embedded work count: 358
- Missing embedding count: 0

## Cluster Before Ranking

- cluster_required_before_ranking: False

## DB Write Scope

- Writes enabled: True
- Allowed tables: embeddings
- ranking_runs written: False
- paper_scores written: False
- production tables modified: False

## Next Stage

run_fresh_hybrid_product_candidate_ranking_v1

## Not Ranking Yet / Not Shadow / Not Production

- Controlled embedding generation for one eval-only fresh hybrid snapshot only.
- Embeddings use existing corpus-v2 title+abstract text rules and persistence machinery.
- No clustering, ranking run, paper_scores, label import, hybrid scoring, shadow, or production changes.
- Snapshot remains eval-only / fresh hybrid validation candidate source; it is not a production/default corpus switch.
- Embedding coverage is a pipeline readiness artifact, not validation or ranking evidence.
- No shadow or production authorization.
