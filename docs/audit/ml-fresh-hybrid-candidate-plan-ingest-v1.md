# Fresh Hybrid Candidate Plan Ingest (ml-fresh-hybrid-candidate-plan-ingest-v1)

## Executive Summary

This artifact records the controlled ingest of the committed fresh hybrid candidate plan into a local eval-only source snapshot. It does not hydrate from OpenAlex, embed, rank, score hybrids, import labels, or authorize shadow/production.

- **Dry run:** False
- **Status:** `succeeded`
- **Snapshot version:** `source-snapshot-fresh-hybrid-v1-20260518`
- **Selected total:** 358
- **Snapshot work count:** 358
- **Recommended next stage:** `hydrate_fresh_hybrid_snapshot_metadata_v1`

## Candidate Plan Summary

- Candidate work-set SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`
- Estimated eligible after old-217 exclusion: 143
- Estimated old-217 overlap: 215
- Negative/borderline candidates: 148

## DB Write Scope

- Writes enabled: True
- Allowed tables: source_snapshot_versions, ingest_runs, raw_openalex_works, works
- ranking_runs written: False
- paper_scores written: False
- production tables modified: False

## Counts

- Inserted: 142
- Updated: 216
- Skipped existing/duplicates: 0
- Failed: 0

## Next Stage

hydrate_fresh_hybrid_snapshot_metadata_v1

## Not Ranking / Not Embeddings / Not Shadow / Not Production

- Controlled local Postgres snapshot ingest from committed candidate plan only.
- No OpenAlex/network calls are made by this ingest command.
- No embeddings, clustering, ranking run, paper_scores, label import, hybrid scoring, shadow, or production changes.
- Snapshot is eval-only / fresh hybrid validation candidate source; it is not a production/default corpus switch.
- Old-217 overlap remains recorded in the plan and must be excluded later during materialization/gates.
- No shadow or production authorization.
