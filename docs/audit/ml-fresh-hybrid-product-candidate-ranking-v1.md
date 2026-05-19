# Fresh Hybrid Product-Candidate Ranking (ml-fresh-hybrid-product-candidate-ranking-v1)

## Executive Summary

This artifact records an eval-only product-candidate ranking run for the fresh hybrid confirmation path. It materializes ranking_runs and paper_scores only; it does not validate the hybrid scorer or authorize shadow/production.

- **Status:** `succeeded`
- **Ranking run ID:** `rank-9f4b2a2084`
- **Snapshot version:** `source-snapshot-fresh-hybrid-v1-20260518`
- **Embedding version:** `fresh-hybrid-text-embedding-v1`
- **Ranking version:** `fresh-hybrid-product-candidate-ranking-v1`
- **Cluster version:** `None`
- **Total candidate works:** 358
- **paper_scores written:** 797
- **Recommended next stage:** `rerun_fresh_product_candidate_ranking_source_after_ranking_v1`

## Paper Scores By Family

- bridge: 358
- emerging: 358
- undercited: 81

## DB Write Scope

- Writes enabled: True
- Allowed tables: ranking_runs, paper_scores
- ranking_runs written: True
- paper_scores written: True
- production tables modified: False
- works modified: False
- embeddings modified: False

## Source Discovery And Materialize Handoff

Fresh source discovery:

```powershell
py -m pipeline.cli ml-fresh-product-candidate-ranking-source --fresh-eval-labeling-plan ../../docs/audit/ml-fresh-eval-labeling-plan-hybrid-v1.json --fresh-surface-policy ../../docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json --label-dataset ../../docs/audit/ml-label-dataset-v8.json --conflict-policy ../../docs/audit/ml-label-conflict-policy.md --ranking-run-id rank-9f4b2a2084 --family emerging --output ../../docs/audit/ml-fresh-product-candidate-ranking-source-v1.json --markdown-output ../../docs/audit/ml-fresh-product-candidate-ranking-source-v1.md
```

Fresh surface materialization:

```powershell
py -m pipeline.cli ml-fresh-eval-surface-hybrid-materialize --fresh-surface-policy ../../docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json --label-dataset ../../docs/audit/ml-label-dataset-v8.json --conflict-policy ../../docs/audit/ml-label-conflict-policy.md --ranking-run-id rank-9f4b2a2084 --family emerging --output ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.json --markdown-output ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.md
```

## Not Hybrid Validation / Not Shadow / Not Production

- Eval-only product-candidate ranking materialization for the fresh hybrid confirmation path.
- Ranking is delegated to existing execute_ranking_run machinery; no production/default ranking pin is changed.
- No new embeddings, hydration, clustering, hybrid validation, label import, API/web change, shadow, or production change.
- cluster_version is omitted because the upstream embedding artifact recorded cluster_required_before_ranking=false.
- paper_scores are source-discovery material for later fresh-surface materialization, not confirmatory validation.
- No shadow or production authorization.
