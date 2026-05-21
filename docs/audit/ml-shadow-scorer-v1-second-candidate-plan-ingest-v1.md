# Second Shadow-Generalization Candidate Plan Ingest (ml-shadow-scorer-v1-second-candidate-plan-ingest-v1)

## Executive Summary

This artifact records the controlled ingest of the committed second hybrid candidate plan into a local eval-only source snapshot. It does not hydrate from OpenAlex, create embeddings, rank, score, import labels, or authorize shadow/production.

- **Dry run:** False
- **Status:** `succeeded`
- **Snapshot version:** `source-snapshot-shadow-generalization-v1-20260521`
- **Selected total:** 528
- **Snapshot work count:** 528
- **Recommended next stage:** `hydrate_second_shadow_generalization_snapshot_metadata_v1`

## Candidate Plan Summary

- Planned candidate work-set SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`
- Estimated confirmatory eligible after exclusions: 168
- Estimated old-217 overlap: 217
- Estimated first-surface overlap: 358
- Borderline/negative selected: 213
- Full underpowered overlap available: False
- Underpowered preview count: 21

## Overlap Tags Preserved

- Old-217 overlap rows in plan: 217
- First validated surface overlap rows in plan: 358
- Underpowered preview-tagged rows in plan: 16
- Confirmatory-after-exclusions rows in plan: 168

## DB Write Scope

- Writes enabled: True
- Allowed tables: source_snapshot_versions, ingest_runs, raw_openalex_works, works
- ranking_runs written: False
- paper_scores written: False
- embeddings written: False
- production tables modified: False

## Counts

- Inserted: 130
- Updated: 398
- Skipped existing/duplicates: 0
- Failed: 0

## Remaining Blockers

- `missing_second_fresh_candidate_source`: False
- `missing_second_surface_ranking_run`: True
- `missing_second_surface_embedding_coverage`: True
- `missing_second_surface_learned_probability_coverage`: True
- `missing_generalization_audit_on_second_surface`: True
- `missing_generalization_audit_gates`: True
- `missing_online_shadow_implementation_disabled_by_default`: True
- `runtime_implementation_authorized`: False
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False

## Not Hydration / Not Ranking / Not Embeddings / Not Shadow / Not Production

- Snapshot ingest only; no OpenAlex enrichment, embeddings, ranking, scorer execution, shadow, or production changes.
- All 528 planned works are ingested, including old-217 and first-surface overlaps for audit traceability.
- Overlap tags are preserved from the plan; confirmatory eligibility is enforced later during materialization/gates.
- Underpowered-source overlap in the plan is preview-limited; this ingest does not claim full 59-work overlap.
- No online shadow, API/web, production default, or runtime implementation is authorized.
