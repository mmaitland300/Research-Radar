# Tiny baseline disagreement audit (emerging)

Compares **within-slice** ordering by persisted `final_score` vs **OOF learned_full linear logits** (same folds as `ml-tiny-baseline`).

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **targets:** good_or_acceptable, surprising_or_useful
- **label_dataset_version:** `ml-label-dataset-v3`
- **top_n listings:** `20`

## Caveats

- This is an offline disagreement audit, not validation of production ranking.
- OOF logits come from the same stratified folds as ml-tiny-baseline learned_full; ranks are within this labeled slice only.
- Labels are single-reviewer audit labels with ranking-selection bias.
- Results must not change production ranking defaults.
- No train/dev/test split is created by this artifact beyond the documented cross-fitting used for OOF scores.

## Target `good_or_acceptable`

- **n_rows:** `60`
- **promoted / demoted / tie:** `38` / `22` / `0`

### Top promotions (learned ranks higher than final_score)

- `1` Δrank=21 final=0.473063 logit=11.5452 — Supervised Contrastive Models for Music Information Retrieval in Classical Persian Music
- `1137` Δrank=20 final=0.478667 logit=13.3328 — Predicting Perceived Semantic Expression of Functional Sounds Using Unsupervised Feature Extraction and Ensemble Learnin
- `6` Δrank=20 final=0.467312 logit=8.92519 — Improving Motif Discovery of Symbolic Polyphonic Music with Motif Note Identification
- `127` Δrank=19 final=0.466432 logit=8.73823 — Reverse Engineering of Music Mixing Graphs With Differentiable Processors and Iterative Pruning
- `8` Δrank=18 final=0.467814 logit=9.03208 — Smartwatch-Based Audio-Gestural Insights in Violin Bow Stroke Analyses
- `4` Δrank=17 final=0.474238 logit=9.72817 — Investigating Auditory-Visual Perception Using Multi-Modal Neural Networks with the SoundActions Dataset
- `113` Δrank=17 final=0.473869 logit=9.56817 — A Similarity-Based Conditioning Method for Controllable Sound Effect Synthesis
- `124` Δrank=14 final=0.467338 logit=8.41549 — Generating Music Reactive Videos by Applying Network Bending to Stable Diffusion
- `101` Δrank=14 final=0.467556 logit=8.50802 — 3D Microphone Array Comparison Part 2: Elicitation of Salient Perceptual Attributes
- `2423` Δrank=11 final=0.177143 logit=6.57174 — Structured and Factorized Multi-Modal Representation Learning for Physiological Affective State and Music Preference Inf
- `2303` Δrank=11 final=0.177384 logit=6.6488 — The sound of emotions: an artificial intelligence approach to predicting emotions from musical selections
- `2294` Δrank=11 final=0.176755 logit=6.44815 — Ontology-Guided Multimodal Framework for Explainable Music Similarity and Recommendation
- `30` Δrank=11 final=0.522208 logit=11.9034 — Multimodal Datasets for Studying Expert Performances of Musical Scores
- `2309` Δrank=10 final=0.170971 logit=2.96757 — SpectTrans: Joint Spectral-Temporal Modeling for Polyphonic Piano Transcription via Spectral Gating Networks
- `2404` Δrank=8 final=0.156715 logit=0.0553655 — Harmony Beyond the Notes: Community Members' Perspectives on the Impact of Classical Music Ensembles on Children and You
- `2325` Δrank=7 final=0.174869 logit=3.03053 — U-MusT: A Unified Framework for Cross-Modal Translation of Score Images, Symbolic Music, and Performance Audio
- `2367` Δrank=6 final=0.171026 logit=2.51527 — Analysis of the impact of machine learning algorithms on the quality of generated sounds
- `9` Δrank=6 final=0.51527 logit=8.87648 — MusiQAl: A Dataset for Music Question-Answering through Audio-Video Fusion
- `2297` Δrank=6 final=0.175314 logit=3.29835 — A multimodal graph-based music auto-tagging framework: integrating social and content intelligence
- `2379` Δrank=5 final=0.149583 logit=-1.40004 — Digital capitalism, platformization and coloniality: an investigation of Jakarta's positioning as a "trigger city" in th

### Top demotions

- `136` Δrank=-38 final=0.969187 logit=1.30099 — Issues and Challenges of Audio Technologies for the Musical Metaverse
- `19` Δrank=-26 final=0.694362 logit=5.56436 — CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research
- `21` Δrank=-26 final=0.690295 logit=4.8464 — PESTO: Real‑Time Pitch Estimation with Self‑Supervised Transposition‑Equivariant Objective
- `137` Δrank=-25 final=0.697223 logit=6.31932 — Toward an Improved Auditory Model for Predicting Binaural Coloration
- `2327` Δrank=-24 final=0.196202 logit=-2.66728 — 4/4 and more, rhythmic complexity more strongly predicts groove in common meters
- `20` Δrank=-23 final=0.590389 logit=4.38124 — STAR Drums: A Dataset for Automatic Drum Transcription
- `2375` Δrank=-22 final=0.246388 logit=-1.51482 — CACA guidelines for music-based interventions in oncology
- `138` Δrank=-21 final=0.603013 logit=6.24557 — Modeling Time-Variant Responses of Optical Compressors With Selective State Space Models
- `2403` Δrank=-17 final=0.343892 logit=-0.113685 — Evaluating the Effects of the Crescendo Programme on Music and Self-Regulation with 5-6-Year-Old Pupils: A Quasi-Experim
- `15` Δrank=-16 final=0.603505 logit=6.85676 — ChoraleBricks: A Modular Multitrack Dataset for Wind Music Research
- `104` Δrank=-12 final=0.606089 logit=7.69524 — On the Lack of a Perceptually Motivated Evaluation Metric for Packet Loss Concealment in Networked Music Performances
- `116` Δrank=-11 final=0.559977 logit=6.85187 — Audio Signal Processing in the Artificial Intelligence Era: Challenges and Directions
- `2395` Δrank=-10 final=0.25513 logit=0.028291 — Crossmodal counterpoint: from music to multimedia - incongruency, cognitive dissonance, irony, and surrealism
- `16` Δrank=-9 final=0.561868 logit=7.21758 — Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data
- `2365` Δrank=-9 final=0.255691 logit=0.990866 — Beyond Acoustics: Capacity Limitations of Linguistic Levels
- `14` Δrank=-5 final=0.561207 logit=7.81506 — The GigaMIDI Dataset with Features for Expressive Music Performance Detection
- `10` Δrank=-4 final=0.515239 logit=7.12918 — Style-Based Composer Identification and Attribution of Symbolic Music Scores: A Systematic Survey
- `2355` Δrank=-1 final=0.153294 logit=-1.25731 — Hashing-Baseline: Rethinking Hashing in the Age of Pretrained Models
- `131` Δrank=-1 final=0.507274 logit=6.87992 — Methods for Pitch Analysis in Contemporary Popular Music: Deviations From 12-Tone Equal Temperament in Vitalic's Work
- `2422` Δrank=-1 final=0.151126 logit=-1.67678 — Identifying statistical indicators of temporal asymmetry using a data-driven approach

## Target `surprising_or_useful`

- **n_rows:** `60`
- **promoted / demoted / tie:** `39` / `18` / `3`

### Top promotions (learned ranks higher than final_score)

- `113` Δrank=19 final=0.473869 logit=4.37781 — A Similarity-Based Conditioning Method for Controllable Sound Effect Synthesis
- `1137` Δrank=18 final=0.478667 logit=4.60829 — Predicting Perceived Semantic Expression of Functional Sounds Using Unsupervised Feature Extraction and Ensemble Learnin
- `1` Δrank=17 final=0.473063 logit=3.80286 — Supervised Contrastive Models for Music Information Retrieval in Classical Persian Music
- `4` Δrank=17 final=0.474238 logit=3.97171 — Investigating Auditory-Visual Perception Using Multi-Modal Neural Networks with the SoundActions Dataset
- `2307` Δrank=16 final=0.175119 logit=2.6585 — MEMA: Multimodal Aesthetic Evaluation of Music in Visual Contexts
- `127` Δrank=16 final=0.466432 logit=3.50169 — Reverse Engineering of Music Mixing Graphs With Differentiable Processors and Iterative Pruning
- `2423` Δrank=16 final=0.177143 logit=2.78097 — Structured and Factorized Multi-Modal Representation Learning for Physiological Affective State and Music Preference Inf
- `2325` Δrank=16 final=0.174869 logit=2.61322 — U-MusT: A Unified Framework for Cross-Modal Translation of Score Images, Symbolic Music, and Performance Audio
- `2303` Δrank=16 final=0.177384 logit=2.8157 — The sound of emotions: an artificial intelligence approach to predicting emotions from musical selections
- `2294` Δrank=15 final=0.176755 logit=2.72529 — Ontology-Guided Multimodal Framework for Explainable Music Similarity and Recommendation
- `2363` Δrank=12 final=0.174582 logit=2.26186 — Explaining cultural emotion in Chinese pop music with multimodal AI: educational and socio-emotional implications
- `30` Δrank=12 final=0.522208 logit=5.16535 — Multimodal Datasets for Studying Expert Performances of Musical Scores
- `2297` Δrank=11 final=0.175314 logit=2.37062 — A multimodal graph-based music auto-tagging framework: integrating social and content intelligence
- `2309` Δrank=10 final=0.170971 logit=1.99336 — SpectTrans: Joint Spectral-Temporal Modeling for Polyphonic Piano Transcription via Spectral Gating Networks
- `9` Δrank=9 final=0.51527 logit=3.93205 — MusiQAl: A Dataset for Music Question-Answering through Audio-Video Fusion
- `2290` Δrank=9 final=0.171083 logit=1.99557 — BACHI: Boundary-Aware Symbolic Chord Recognition Through Masked Iterative Decoding on POP and Classical Music
- `2367` Δrank=7 final=0.171026 logit=1.73395 — Analysis of the impact of machine learning algorithms on the quality of generated sounds
- `11` Δrank=7 final=0.566768 logit=4.8354 — The AI Music Arms Race: On the Detection of AI-Generated Music
- `10` Δrank=6 final=0.515239 logit=3.62846 — Style-Based Composer Identification and Attribution of Symbolic Music Scores: A Systematic Survey
- `2404` Δrank=4 final=0.156715 logit=-0.155047 — Harmony Beyond the Notes: Community Members' Perspectives on the Impact of Classical Music Ensembles on Children and You

### Top demotions

- `20` Δrank=-38 final=0.590389 logit=-0.208339 — STAR Drums: A Dataset for Automatic Drum Transcription
- `136` Δrank=-37 final=0.969187 logit=0.805946 — Issues and Challenges of Audio Technologies for the Musical Metaverse
- `19` Δrank=-32 final=0.694362 logit=1.77356 — CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research
- `21` Δrank=-28 final=0.690295 logit=2.20342 — PESTO: Real‑Time Pitch Estimation with Self‑Supervised Transposition‑Equivariant Objective
- `2327` Δrank=-24 final=0.196202 logit=-1.77279 — 4/4 and more, rhythmic complexity more strongly predicts groove in common meters
- `15` Δrank=-20 final=0.603505 logit=2.39729 — ChoraleBricks: A Modular Multitrack Dataset for Wind Music Research
- `137` Δrank=-19 final=0.697223 logit=2.77018 — Toward an Improved Auditory Model for Predicting Binaural Coloration
- `131` Δrank=-17 final=0.507274 logit=1.25331 — Methods for Pitch Analysis in Contemporary Popular Music: Deviations From 12-Tone Equal Temperament in Vitalic's Work
- `2375` Δrank=-16 final=0.246388 logit=-0.76265 — CACA guidelines for music-based interventions in oncology
- `2403` Δrank=-11 final=0.343892 logit=0.20161 — Evaluating the Effects of the Crescendo Programme on Music and Self-Regulation with 5-6-Year-Old Pupils: A Quasi-Experim
- `138` Δrank=-10 final=0.603013 logit=2.95641 — Modeling Time-Variant Responses of Optical Compressors With Selective State Space Models
- `2395` Δrank=-8 final=0.25513 logit=0.680608 — Crossmodal counterpoint: from music to multimedia - incongruency, cognitive dissonance, irony, and surrealism
- `2365` Δrank=-8 final=0.255691 logit=0.762899 — Beyond Acoustics: Capacity Limitations of Linguistic Levels
- `104` Δrank=-7 final=0.606089 logit=3.57386 — On the Lack of a Perceptually Motivated Evaluation Metric for Packet Loss Concealment in Networked Music Performances
- `124` Δrank=-3 final=0.467338 logit=2.25184 — Generating Music Reactive Videos by Applying Network Bending to Stable Diffusion
- `6` Δrank=-3 final=0.467312 logit=2.24742 — Improving Motif Discovery of Symbolic Polyphonic Music with Motif Note Identification
- `8` Δrank=-3 final=0.467814 logit=2.33042 — Smartwatch-Based Audio-Gestural Insights in Violin Bow Stroke Analyses
- `128` Δrank=-2 final=0.51648 logit=3.01813 — Designing Neural Synthesizers for Low-Latency Interaction
