# Corpus expansion preview

Generated: `2026-04-28T03:37:49Z`
OpenAlex mode: **live**  
**Suggested target size (works):** [200, 500]  
**expand_before_next_bridge_weight_experiment:** True
**keep_current_corpus_for_smoke_only:** True

## Recommendation (non-binding)
- First-pass bucket ids: ['core_mir_existing_sources', 'ismir_proceedings_or_mir_conference', 'audio_ml_signal_processing', 'music_recommender_systems']
- Defer (noise-prone) bucket ids: ['ethics_law_fairness_user_studies', 'cultural_computational_musicology']
- Stricter bridge eligibility on next zero-weight run: **True**

## Versioning implications
- **new_corpus_snapshot_version:** Ingesting an expanded source set or policy will produce a new source_snapshot_version and corpus identity for downstream steps.
- **repaired_work_text_artifact:** If text repair runs, corrected title/abstract text is tied to the snapshot; label embedding inputs accordingly.
- **new_embedding_version:** New embedding_version label after text change or model change; required before clustering and ranking on the new pool.
- **new_cluster_version:** New cluster_version after re-embed; bridge_score depends on cluster boundary definitions.
- **new_zero_bridge_ranking_version:** A zero–bridge-weight ranking run should be the baseline before any bridge weight experiment on the new pool.
- **fresh_review_worksheets_and_summaries:** Manual review CSVs, family summaries, and rollups are invalid across corpus pools; regenerate after a new snapshot.

## Caveats
- The current ~59-work corpus is suitable for smoke, plumbing, and demo evidence only, not for strong ML-quality generalization claims.
- Expanding the corpus changes candidate pools and label distributions; do not treat metrics from the old 59-work pool and a new pool as same-pool performance.
- A serious expansion is followed (in order) by text repair, embedding coverage checks, clustering, a zero–bridge-weight ranking run, bridge diagnostics, and fresh labels before comparing bridge head distinctness.
- This preview is not an ingest or policy decision by itself; apply topic gates, exclusions, and source policy from CorpusPolicy in code before committing a snapshot.
- Preview counts and samples are from OpenAlex list endpoints; deduplication, retraction, and final inclusion rules may reduce realized corpus size.

This document is a planning note only. It is **not** a scientific validation, benchmark result, or ingest commitment.

## Buckets (summary)
### core_mir_existing_sources
- **Estimated count (OpenAlex list meta):** 576
- **Sample size this run:** 20 works (preview only)
### ismir_proceedings_or_mir_conference
- **Estimated count (OpenAlex list meta):** 3688
- **Sample size this run:** 20 works (preview only)
### audio_ml_signal_processing
- **Estimated count (OpenAlex list meta):** 11833
- **Sample size this run:** 20 works (preview only)
### music_recommender_systems
- **Estimated count (OpenAlex list meta):** 3379
- **Sample size this run:** 20 works (preview only)
### cultural_computational_musicology
- **Estimated count (OpenAlex list meta):** 1337
- **Sample size this run:** 20 works (preview only)
### ethics_law_fairness_user_studies
- **Estimated count (OpenAlex list meta):** 11719
- **Sample size this run:** 20 works (preview only)
### symbolic_music_and_harmony
- **Estimated count (OpenAlex list meta):** 4161
- **Sample size this run:** 20 works (preview only)
### source_separation_benchmarks
- **Estimated count (OpenAlex list meta):** 4950
- **Sample size this run:** 20 works (preview only)
