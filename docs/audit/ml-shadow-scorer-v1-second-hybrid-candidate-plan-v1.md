# ML Shadow Scorer v1 Second Hybrid Candidate Plan (ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1)

## Executive Summary

This is a dry-run OpenAlex candidate acquisition plan for a second fresh surface. It does not ingest, write a database, rank, embed, apply a learned scorer, execute shadow scoring, or authorize production.

- Selected candidates: 528
- Estimated confirmatory-eligible after old/first-surface exclusions: 168
- Candidate threshold plausibly met: True
- Expected next stage: `ingest_second_hybrid_candidate_plan_as_snapshot_v1`
- Shadow scoring allowed: False
- Production default allowed: False

## Planning Context

The best existing distinct source is `rank-3904fec89d` with 43 confirmatory-eligible works, leaving a gap of 57 against the policy minimum of 100.

## Candidate Plan Size And SHA

- Selected total: 528 (target range 180-600)
- Planned candidate work-set SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`

## Overlap Estimates

- Old 217 overlap estimate: 217
- First validated surface overlap estimate: 358
- Full underpowered overlap available: False
- Underpowered preview overlap estimate: 16

## Bucket Composition

| Rollup | Selected | Confirmatory after exclusions |
| --- | ---: | ---: |
| `core_product_candidate` | 240 | 78 |
| `borderline_or_negative_candidate` | 213 | 64 |
| `recommender_or_evaluation_candidate` | 75 | 26 |
| `MIR/audio_candidate` | 453 | 142 |

### Raw Buckets

| Bucket | Selected | Confirmatory after exclusions | Old overlap | First-surface overlap | Borderline intent |
| --- | ---: | ---: | ---: | ---: | --- |
| `audio_ml_signal_processing` | 85 | 20 | 29 | 65 | True |
| `core_mir_existing_sources` | 120 | 38 | 80 | 80 | False |
| `cultural_computational_musicology` | 8 | 0 | 0 | 8 | True |
| `ismir_proceedings_or_mir_conference` | 120 | 40 | 62 | 80 | False |
| `music_recommender_systems` | 75 | 26 | 17 | 49 | False |
| `source_separation_benchmarks` | 60 | 22 | 11 | 38 | True |
| `symbolic_music_and_harmony` | 60 | 22 | 18 | 38 | True |

## Follow-Ups

- Learned probability coverage must come later from an approved frozen scorer application to pre-existing embeddings.
- Labels may still block future audit execution; labels are metric-only and never scoring features.

## Not Ingest / Not Runtime / Not Shadow / Not Production

- Dry-run plan only; no ingest, DB writes, ranking, embeddings, scorer execution, runtime, shadow/prod, or API changes.
- OpenAlex metadata may drift before later ingest.
- High overlap with the first validated surface may force query revision even with target-max 600.
- Underpowered-source overlap may be preview-limited; preview overlap must not be treated as full 59-work pool overlap.
- Candidate selection is label-blind; v10 labels are not used to choose works.
