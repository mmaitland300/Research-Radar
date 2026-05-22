# Second Shadow-Generalization Product-Candidate Ranking (ml-shadow-scorer-v1-second-product-candidate-ranking-v1)

## Executive Summary

This artifact records an eval-only product-candidate ranking run for the second shadow-generalization source. It materializes ranking_runs and paper_scores only; it does not generate learned probabilities, execute the shadow scorer, or authorize shadow/production.

- **Status:** `succeeded`
- **Ranking run ID:** `rank-83787b91ef`
- **Snapshot version:** `source-snapshot-shadow-generalization-v1-20260521`
- **Embedding version:** `shadow-generalization-text-embedding-v1`
- **Ranking version:** `shadow-generalization-product-candidate-ranking-v1`
- **Cluster version:** `None`
- **Total candidate works:** 528
- **Emerging family work count:** 528
- **paper_scores written:** 1177
- **Recommended next stage:** `rerun_second_shadow_generalization_surface_discovery_v1`

## Paper Scores By Family

- bridge: 528
- emerging: 528
- undercited: 121

## DB Write Scope

- Writes enabled: True
- Allowed tables: ranking_runs, paper_scores
- ranking_runs written: True
- paper_scores written: True
- source_snapshot_versions written: False
- ingest_runs written: False
- raw_openalex_works written: False
- works modified: False
- embeddings modified: False
- production tables modified: False

## Second-Surface Discovery Handoff

Rerun second-surface discovery:

```powershell
py -m pipeline.cli ml-shadow-scorer-generalization-second-surface --generalization-audit-plan ../../docs/audit/ml-shadow-scorer-v1-generalization-audit-v1.json --online-shadow-policy ../../docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json --fresh-surface-policy ../../docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json --label-dataset ../../docs/audit/ml-label-dataset-v10.json --conflict-policy ../../docs/audit/ml-label-conflict-policy.md --offline-production-candidate-scoring-v3 ../../docs/audit/ml-offline-production-candidate-scoring-v3.json --first-validated-surface ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.json --family emerging --output ../../docs/audit/ml-shadow-scorer-v1-generalization-second-surface-v1.json --markdown-output ../../docs/audit/ml-shadow-scorer-v1-generalization-second-surface-v1.md
```

## Remaining Blockers

- `missing_second_fresh_candidate_source`: False
- `missing_second_surface_embedding_coverage`: False
- `missing_second_surface_ranking_run`: False
- `missing_second_surface_learned_probability_coverage`: True
- `missing_generalization_audit_on_second_surface`: True
- `missing_generalization_audit_gates`: True
- `runtime_implementation_authorized`: False
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False

## Not Learned Probability / Not Shadow / Not Production

- Eval-only product-candidate ranking materialization for the second shadow-generalization source.
- Ranking materializes heuristic final_score paths only; no learned audit_embedding_probability_work is generated.
- paper_scores are discovery inputs, not confirmatory validation or shadow execution.
- The new ranking_run_id must remain distinct from rank-9f4b2a2084.
- No hydration, snapshot/work/raw writes, embeddings, scorer execution, label ingest, online shadow, API/web, or production/default change.
- No shadow or production authorization.
