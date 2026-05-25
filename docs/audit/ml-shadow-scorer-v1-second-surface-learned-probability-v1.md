# Second-Surface Learned Probability Coverage (ml-shadow-scorer-v1-second-surface-learned-probability-v1)

## Executive Summary

This artifact applies the frozen offline audit embedding scorer to existing second-surface embeddings and writes audit-only learned probabilities. It does not refit, train, generate embeddings, write database rows, rerun discovery, or authorize shadow/production behavior.

- Status: `succeeded`
- Ranking run: `rank-83787b91ef`
- Family: `emerging`
- Candidate SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`
- Candidate pool: 528
- Learned-probability coverage: 528 / 528
- Recommended next stage: `extend_second_surface_probability_probe_and_rerun_discovery_v1`

## Evidence Chain

- Corpus snapshot: `source-snapshot-shadow-generalization-v1-20260521`
- Embedding version: `shadow-generalization-text-embedding-v1`
- Scorer version: `ml-offline-audit-embedding-scorer-v2`
- Scorer fit mode: `holdout_bound_train_only`
- Scorer target: `good_or_acceptable`
- Labels used for scoring: False

## Frozen Scorer Contract

- `approved_embedding_version`: shadow-generalization-text-embedding-v1
- `approved_embeddings_artifact`: docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json
- `approved_scorer`: ml-offline-audit-embedding-scorer-v2
- `approved_scorer_artifact_type`: ml_offline_audit_embedding_scorer
- `future_execution_command`: ml-shadow-scorer-second-surface-learned-probability-apply
- `future_execution_output_artifact`: docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json
- `future_probe_update`: extend discovery _approved_probability_probe to read the new artifact keyed by ranking_run_id + candidate_pool_work_set_sha256; do not reuse first-surface audit artifacts
- `must_not_refit`: True
- `must_not_regenerate_embeddings`: True
- `must_not_use_v11_labels_as_scorer_features`: True
- `output_field`: audit_embedding_probability_work
- `post_execution_discovery_rerun`: ml-shadow-scorer-generalization-second-surface with ml-label-dataset-v11.json; expected status selected_ready_for_generalization_audit
- `target_coverage`: {'candidate_pool_work_count': 528, 'coverage_rate': 1.0, 'covered_work_count': 528}
- `frozen_apply_only`: True
- `labels_not_used_for_scoring`: True
- `no_db_writes`: True

## DB Read Scope

- Reads enabled: True
- Source tables: ranking_runs, paper_scores, works, embeddings
- Writes enabled: False
- Write tables: []

## Coverage Counts

- Final score coverage: 528 / 528
- Embedding coverage: 528 / 528
- Learned probability coverage: 528 / 528
- Missing embedding count: 0
- Missing probability count: 0

## Probability Distribution

- `min`: 1.3694895944786864e-05
- `p25`: 0.187278332413795
- `median`: 0.7229454578547996
- `p75`: 0.9743837686534901
- `max`: 0.9999245310558568
- `mean`: 0.5934206384326456
- `count`: 528

## Top 20 Probability Preview

| Rank | Work | Probability | Final score | Title |
| ---: | --- | ---: | ---: | --- |
| 1 | `W4417094688` | 0.999925 | 0.174957 | U-MusT: A Unified Framework for Cross-Modal Translation of Score Images, Symbolic Music, and Performance Audio |
| 2 | `W4408772031` | 0.999910 | 0.594650 | CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research |
| 3 | `W4416695332` | 0.999881 | 0.171120 | From Discord to Harmony: Decomposed Consonance-based Training for Improved Audio Chord Estimation |
| 4 | `W7119511988` | 0.999879 | 0.167584 | End-to-End Full-Page Optical Music Recognition for Pianoform Sheet Music |
| 5 | `W4416369344` | 0.999873 | 0.168313 | Detecting Notational Errors in Digital Music Scores |
| 6 | `W4417301638` | 0.999872 | 0.167965 | Text2midi-InferAlign: Improving Symbolic Music Generation with Inference-Time Alignment |
| 7 | `W4415011197` | 0.999859 | 0.168617 | An implicit layout-aware transformer for full-page end-to-end optical music recognition |
| 8 | `W7119099299` | 0.999853 | 0.431143 | Supervised Contrastive Models for Music Information Retrieval in Classical Persian Music |
| 9 | `W7125624381` | 0.999834 | 0.160069 | Transcription automatique des performances musicales symboliques et polyphoniques |
| 10 | `W7160512765` | 0.999820 | 0.158248 | Hardware Accelerator Design for MUSIC-DOA Estimation with Bilateral Jacobi Optimization |
| 11 | `W4416267785` | 0.999721 | 0.433627 | ImproVision Equilibrium: Toward Multimodal Musical Human-Machine Interaction |
| 12 | `W4412377260` | 0.999711 | 0.171688 | MIDI-Zero: A MIDI-driven Self-Supervised Learning Approach for Music Retrieval |
| 13 | `W7143719133` | 0.999689 | 0.169481 | The Hi-Audio online platform for recording and distributing multi-track music datasets |
| 14 | `W4406890357` | 0.999648 | 0.166162 | Separate this, and all of these Things Around It: Music Source Separation Via Hyperellipsoidal Queries |
| 15 | `W4402645135` | 0.999583 | 0.508696 | BPSD: A Coherent Multi-Version Dataset for Analyzing the First Movements of Beethoven's Piano Sonatas |
| 16 | `W4410091503` | 0.999579 | 0.501780 | Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data |
| 17 | `W7106686878` | 0.999526 | 0.167809 | Enabling Empirical Analysis of Piano Performance Rehearsal With the Rach3 MIDI Dataset |
| 18 | `W4401909510` | 0.999511 | 0.459916 | Jazz Trio Database: Automated Annotation of Jazz Piano Trio Recordings Processed Using Audio Source Separation |
| 19 | `W7131093365` | 0.999507 | 0.162773 | Towards common ground in notation for augmented instruments |
| 20 | `W7160852273` | 0.999465 | 0.168915 | Multimodal Automatic Music Transcription Using Piano Audio and Hand-Skeleton Information |

## Blockers

- `missing_second_surface_learned_probability_coverage`: False
- `missing_generalization_audit_on_second_surface`: True
- `missing_generalization_audit_gates`: True
- `missing_online_shadow_implementation_disabled_by_default`: True
- `missing_shadow_runtime_isolation_verification`: True
- `missing_production_readiness_authorization`: True
- `runtime_implementation_authorized`: False
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False

## Next Rerun Command

After extending the discovery probe to include this artifact, rerun `ml-shadow-scorer-generalization-second-surface` pinned to `rank-83787b91ef` with `ml-label-dataset-v11.json`.

## Caveats

- Offline audit artifact only.
- Applies the frozen scorer to existing embeddings; does not refit or train.
- Does not generate embeddings.
- Does not write database rows.
- Does not authorize online shadow, API/web, production default, or user-visible ranking changes.
- Labels are provenance only and are not scoring features.
