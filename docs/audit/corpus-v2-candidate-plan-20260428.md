# Corpus v2 candidate plan (dry-run)

This document is **planning output only**. It does **not** validate benchmarks, retrieval quality, or bridge readiness. **No database writes** were performed to produce this artifact.

## Totals

- **selected_total:** `217` (target range `200`–`500`)
- **contact_mode:** `cli` (raw mailto is not stored in this file)

## Selected by bucket

- **core_mir_existing_sources:** selected `80` (raw fetched `100`, passed filter `94`, cap `80`)
- **ismir_proceedings_or_mir_conference:** selected `62` (raw fetched `100`, passed filter `67`, cap `80`)
- **audio_ml_signal_processing:** selected `21` (raw fetched `100`, passed filter `30`, cap `60`)
- **music_recommender_systems:** selected `19` (raw fetched `100`, passed filter `25`, cap `50`)
- **symbolic_music_and_harmony:** selected `20` (raw fetched `100`, passed filter `29`, cap `40`)
- **source_separation_benchmarks:** selected `15` (raw fetched `100`, passed filter `25`, cap `40`)
- **cultural_computational_musicology:** selected `0` (raw fetched `100`, passed filter `0`, cap `25`)
- **ethics_law_fairness_user_studies:** selected `0` (raw fetched `100`, passed filter `4`, cap `25`)

## Recommended first-pass corpus-v2 scope

Approve a corpus-v2 ingest policy update that uses this candidate set (or a subset) as the first expansion tranche, then create a new snapshot version, re-embed, re-cluster, and run a zero–bridge-weight ranking before any bridge-weight tuning.

## Noisy / defer-heavy buckets

- Buckets `cultural_computational_musicology` and `ethics_law_fairness_user_studies` use stricter music/MIR hooks in this dry-run; low counts are expected.

## Dedup statistics

- **drops_by_openalex_id:** `42`
- **drops_by_doi:** `0`
- **drops_by_normalized_title:** `1`
- **unique_openalex_ids_kept:** `217`

## Accepted examples (first 5)

- `https://openalex.org/W7147047272` — 'Evaluation and Prediction of Perceived Naturalness for Source Directivity With a Visible Virtual Source' (`core_mir_existing_sources`)
- `https://openalex.org/W7147215911` — 'Continuous and Overall Evaluation of Spatial Audio Reproduction Systems With Spatially Dynamic Content' (`core_mir_existing_sources`)
- `https://openalex.org/W7147440360` — 'A Low-Dimensional Projection of Sound Absorption Coefficients for a Scattering Delay Network Reverberator' (`core_mir_existing_sources`)
- `https://openalex.org/W7134122976` — '3D Microphone Array Comparison Part 2: Elicitation of Salient Perceptual Attributes' (`core_mir_existing_sources`)
- `https://openalex.org/W7133229896` — 'Predicting Perceived Semantic Expression of Functional Sounds Using Unsupervised Feature Extraction and Ensemble Learning' (`core_mir_existing_sources`)

## Rejected examples (first 3 per bucket, truncated)

### core_mir_existing_sources
- `https://openalex.org/W4414799717` — 'An Automatic Mixing Speech Enhancement System for Information Integrity' — **explicit_exclusion_term**
- `https://openalex.org/W4414012218` — 'Head-Related Transfer Function Upsampling Using an Autoencoder-Based Generative Adversarial Network With Evaluation Framework' — **explicit_exclusion_term**
- `https://openalex.org/W4407726640` — 'Interacting with Annotated and Synchronized Music Corpora on the Dezrann Web Platform' — **explicit_exclusion_term**
### ismir_proceedings_or_mir_conference
- `https://openalex.org/W7155372642` — 'Automated detection of stereotyped animal sounds using data augmentation and transfer learning' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W4416383433` — 'Leveraging Whisper Embeddings For Audio-Based Lyrics Matching' — **explicit_exclusion_term**
- `https://openalex.org/W7140282278` — 'FGIM: a Fast Graph-based Indexes Merging Framework for Approximate Nearest Neighbor Search' — **noise_generic_database_without_music_hook**
### audio_ml_signal_processing
- `https://openalex.org/W7155655395` — 'Understanding How Teachers Define and Employ Civics Education in Non-Elective Social Studies Classrooms' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7134469035` — 'English to Central Kurdish Speech Translation: Corpus Creation, Evaluation, and Orthographic Standardization' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7153680773` — 'Perceptions of Effective Co-Teaching Partnerships in High School Inclusive Classrooms' — **no_strong_topic_or_bucket_allow_signal**
### music_recommender_systems
- `https://openalex.org/W7154844529` — 'Comparison between Pulmonary Rehabilitation and Dance in Patients with Chronic Obstructive Pulmonary Disease: A Randomized Controlled Clinical Trial.' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7152625278` — 'The digital infrastructures of music scenes: Perspectives from the Global South' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7152579252` — 'Manx language for all: community, cooperation and creativity in the 21st century' — **no_strong_topic_or_bucket_allow_signal**
### symbolic_music_and_harmony
- `https://openalex.org/W7155515923` — 'Moralized Parental Violence and the Ethics of Reconciliation in Sinophone Family Cinema' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7154978457` — 'The Ethnographer as Detective: Indiciary Paradigm and Abduction' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7151958866` — 'The aesthetic triad and traditional Chinese art symbols: a theoretical framework' — **no_strong_topic_or_bucket_allow_signal**
### source_separation_benchmarks
- `https://openalex.org/W7155497153` — 'Facial emotion-based movie recommendation system using optimized compound scaling neural network with polynomial and RBF kernels' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7155518048` — 'Computational approaches to controversy detection: a systematic review' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7155541772` — 'Effects of Cognitive Style and Evaluation Context on Hedonic and Sensory Perception of Café Latte: A Comparison of Sensory Booth, Real-Life, and Mixed Reality Environments' — **no_strong_topic_or_bucket_allow_signal**
### cultural_computational_musicology
- `https://openalex.org/W7155391705` — 'Decoding the evolution of melodic and harmonic structure of Western music through the lens of network science' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W4417301638` — 'Text2midi-InferAlign: Improving Symbolic Music Generation with Inference-Time Alignment' — **defer_bucket_weak_musicology_hook**
- `https://openalex.org/W7154260406` — 'A Multidimensional MIR Analysis of Acoustic, Linguistic and Cultural Gaps Between Maskandi and Western Music Genres' — **defer_bucket_weak_musicology_hook**
### ethics_law_fairness_user_studies
- `https://openalex.org/W7117442860` — 'Generative machine learning in professional work and professional service firms : a research agenda' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W4404570405` — 'Bias in Large Language Models: Origin, Evaluation, and Mitigation' — **no_strong_topic_or_bucket_allow_signal**
- `https://openalex.org/W7155499410` — 'Pretty or practical? The role of gender stereotypes in shaping design concepts' — **no_strong_topic_or_bucket_allow_signal**

## Caveats

- Dry-run only: no Postgres writes, no snapshot, no embeddings, clustering, or ranking.
- Candidate list is not a benchmark and does not validate retrieval or bridge quality.
- Old vs new corpus metrics must not be compared as same-pool performance.

## Versioning implications

- **new_corpus_snapshot_version:** required_before_ingest
- **new_embedding_version:** required_after_snapshot
- **new_cluster_version:** required_after_embeddings
- **new_zero_bridge_ranking_version:** required_before_bridge_weight_experiments
