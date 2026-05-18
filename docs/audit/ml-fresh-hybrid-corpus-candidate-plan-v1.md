# Fresh Hybrid Corpus Candidate Plan (ml-fresh-hybrid-corpus-candidate-plan-v1)

## Executive Summary

This is a dry-run OpenAlex candidate plan for expanding the supply side of the fresh hybrid confirmation path. It does not write Postgres, create a snapshot, rank, train, label, score hybrids, or authorize shadow/production.

- **Selected candidates:** 358
- **Estimated eligible after old-217 exclusion:** 143
- **Candidate threshold plausibly met:** True
- **Expected next stage:** `ingest_fresh_hybrid_candidate_plan_as_snapshot_v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Why Local DB Is Blocked

The best existing local source is `rank-3904fec89d` with 44 confirmatory-eligible works, leaving a candidate gap of 56 against the policy minimum of 100.

## Candidate Plan Size And SHA

- Selected total: 358 (target range 160–500)
- Selected candidate work-set SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`

## Overlap Estimates

- Old 217 overlap estimate: 215
- Underpowered 44 overlap estimate: 1
- New candidates excluding underpowered source: 142

## Bucket Composition

| Bucket | Selected | Confirmatory after old exclusion | Old overlap | Underpowered overlap | Borderline intent |
| --- | ---: | ---: | ---: | ---: | --- |
| `audio_ml_signal_processing` | 60 | 33 | 27 | 0 | True |
| `core_mir_existing_sources` | 80 | 2 | 78 | 0 | False |
| `cultural_computational_musicology` | 8 | 8 | 0 | 1 | True |
| `ismir_proceedings_or_mir_conference` | 80 | 18 | 62 | 0 | False |
| `music_recommender_systems` | 50 | 32 | 18 | 0 | False |
| `source_separation_benchmarks` | 40 | 29 | 11 | 0 | True |
| `symbolic_music_and_harmony` | 40 | 21 | 19 | 0 | True |

## Negative / Borderline Intent

- Present: True
- Selected count: 148

## Readiness Estimate

- Enough candidates for next ingest: True
- Expected next stage: `ingest_fresh_hybrid_candidate_plan_as_snapshot_v1`

## Materialization Path

1. ingest_fresh_hybrid_candidate_plan_as_snapshot_v1
2. hydrate metadata/text if needed
3. embed snapshot if required by ranking path
4. run product-candidate ranking-run with eval-only namespacing
5. rerun ml-fresh-product-candidate-ranking-source
6. rerun ml-fresh-eval-surface-hybrid-materialize
7. labeling worksheet if materialized_needs_labels
8. hybrid validation only after materializer plus policy thresholds

## Not Validation / Not Shadow / Not Production

- Dry-run candidate plan only; no Postgres writes, source snapshot, ranking run, embeddings, or label import.
- OpenAlex metadata is read-only and may drift before a later ingest/snapshot step.
- Candidate selection is label-blind; v8 labels are not used to choose candidates.
- Old 217-work confirmatory surface is excluded from readiness estimates.
- Underpowered 44-work source overlap is reported separately.
- Not hybrid validation, not live recommender validation, and not shadow/production authorization.
