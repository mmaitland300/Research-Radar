# Second Shadow-Generalization Snapshot Embeddings (ml-shadow-scorer-v1-second-snapshot-embeddings-v1)

## Executive Summary

This artifact records title+abstract embedding generation for the eval-only second shadow-generalization snapshot. It does not hydrate, rank, write paper_scores, generate learned probabilities, execute scorers, import labels, or authorize shadow/production.

- **Snapshot version:** `source-snapshot-shadow-generalization-v1-20260521`
- **Embedding version:** `shadow-generalization-text-embedding-v1`
- **Status:** `succeeded`
- **Mock embeddings:** False
- **Dry run:** False
- **Works considered:** 528
- **Embeddings written / skipped / failed:** 528 / 0 / 0
- **Full snapshot embedding coverage:** True
- **Recommended next stage:** `run_second_shadow_generalization_product_candidate_ranking_v1`

## Model And Dimensions

- Provider: `openai`
- Model: `text-embedding-3-small`
- Dimensions: 1536
- Text source: `title_abstract`

## Coverage

- Snapshot work count: 528
- Embedded work count: 528
- Missing embedding count: 0

## DB Write Scope

- Writes enabled: True
- Allowed tables: embeddings
- source_snapshot_versions written: False
- ingest_runs written: False
- works written: False
- ranking_runs written: False
- paper_scores written: False
- embeddings written: True
- production tables modified: False

## Remaining Blockers

- `missing_second_fresh_candidate_source`: False
- `missing_second_surface_embedding_coverage`: False
- `missing_second_surface_ranking_run`: True
- `missing_second_surface_learned_probability_coverage`: True
- `missing_generalization_audit_on_second_surface`: True
- `missing_generalization_audit_gates`: True
- `runtime_implementation_authorized`: False
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False

## Not Ranking / Not Learned Probability / Not Shadow / Not Production

- Controlled embedding generation for one eval-only shadow-generalization snapshot only.
- The embedding_version is intentionally distinct from fresh-hybrid-text-embedding-v1.
- Embedding coverage is pipeline readiness only, not generalization proof.
- No OpenAlex hydration, ranking run, paper_scores, learned probability generation, scorer execution, label ingest, shadow runtime, or production changes.
- No online shadow, API/web, production default, or runtime implementation is authorized.
