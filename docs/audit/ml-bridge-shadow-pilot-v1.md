# Bridge shadow pilot v1

Offline re-ranking: frozen v2 ML model + bridge_score → hybrid_bridge_score_50_50 across all 528 Bridge candidates. Not validation; no serving change.

- Ranking run (bridge_score source): `rank-5a7efa5ca3`
- Embedding version: `shadow-generalization-text-embedding-v1`
- Candidates: 528
- bridge_score coverage: 528/528 (100.0%)

## Top-20 comparison

- Promoted (hybrid top-20, not current top-20): **20**
- Demoted (current top-20, not hybrid top-20): **20**
- Stable (in both): **0**

### Promoted papers

  - rank 377 → 1 | ml=1.000 bridge=0.990 | Potential and Pitfalls of Audio as Data for Political Resear
  - rank 186 → 2 | ml=1.000 bridge=0.990 | Sparse Autoencoders Make Audio Foundation Models More Explai
  - rank 183 → 3 | ml=0.999 bridge=0.967 | The Games We Play: Exploring the Impact of ISMIR on Musicolo
  - rank 505 → 4 | ml=0.998 bridge=0.983 | Neural Audio Synthesis for Sound Effects: A Scope Review
  - rank 311 → 5 | ml=1.000 bridge=0.944 | The Sound of Water: Inferring Physical Properties from Pouri
  - rank 32 → 6 | ml=0.998 bridge=0.975 | Estimation and Restoration of Unknown Nonlinear Distortion U
  - rank 385 → 7 | ml=0.998 bridge=0.968 | Melodies of the forest: Nature as an improvisational space f
  - rank 147 → 8 | ml=0.999 bridge=0.943 | Enabling interoperable human-AI teaming for automation in co
  - rank 453 → 9 | ml=0.999 bridge=0.945 | Music, humans, and machines: initial reflections for the dev
  - rank 321 → 10 | ml=0.999 bridge=0.934 | TalkPlay-Tools: Conversational Music Recommendation with LLM
  - rank 54 → 11 | ml=0.998 bridge=0.941 | Distilling DDSP: Exploring Real-Time Audio Generation on Emb
  - rank 48 → 12 | ml=0.996 bridge=0.949 | GOLF: A Singing Voice Synthesiser with Glottal Flow Wavetabl
  - rank 61 → 13 | ml=0.997 bridge=0.937 | Charting the Universe of Metal Music Lyrics and Analyzing Th
  - rank 91 → 14 | ml=1.000 bridge=0.904 | Analysis of Human Participants in the Journal of the Audio E
  - rank 470 → 15 | ml=0.995 bridge=0.957 | HarmonyTok: Comparing Methods for Harmony Tokenization for M
  - rank 130 → 16 | ml=0.997 bridge=0.932 | Beyond Acoustics: Capacity Limitations of Linguistic Levels
  - rank 303 → 17 | ml=0.997 bridge=0.935 | A neutrosophic clustering approach to handle recommendation 
  - rank 26 → 18 | ml=0.993 bridge=0.978 | A Lightweight Two‑Branch Architecture for Multi‑Instrument T
  - rank 369 → 19 | ml=1.000 bridge=0.905 | Fund Similarity: A Use of Bipartite Graphs
  - rank 398 → 20 | ml=1.000 bridge=0.896 | Logical Metainferentialism

### Demoted papers

  - rank 14 → 42 | ml=0.998 bridge=0.893 | Issues and Challenges of Audio Technologies for the Musical 
  - rank 10 → 45 | ml=0.993 bridge=0.931 | The AI Music Arms Race: On the Detection of AI-Generated Mus
  - rank 13 → 46 | ml=0.995 bridge=0.909 | Reverse Engineering of Music Mixing Graphs With Differentiab
  - rank 3 → 52 | ml=0.995 bridge=0.905 | Modeling Time-Variant Responses of Optical Compressors With 
  - rank 1 → 70 | ml=0.994 bridge=0.897 | Physical Modeling of a Spring Reverb Tank Incorporating Heli
  - rank 7 → 84 | ml=0.996 bridge=0.884 | Inferring Communities of Medieval Music Manuscripts Using St
  - rank 20 → 171 | ml=0.996 bridge=0.821 | ImproVision Equilibrium: Toward Multimodal Musical Human-Mac
  - rank 8 → 199 | ml=0.992 bridge=0.827 | Beyond a Western Center of Music Information Retrieval: A Bi
  - rank 19 → 244 | ml=0.007 bridge=0.948 | PESTO: Real‑Time Pitch Estimation with Self‑Supervised Trans
  - rank 9 → 368 | ml=0.000 bridge=0.895 | Analysis of Various 3D Acquisition Techniques and Mesh Diffe
  - rank 12 → 411 | ml=0.002 bridge=0.857 | STAR Drums: A Dataset for Automatic Drum Transcription
  - rank 4 → 415 | ml=0.009 bridge=0.833 | Toward an Improved Auditory Model for Predicting Binaural Co
  - rank 15 → 437 | ml=0.004 bridge=0.834 | High-Resolution Directivity Measurements of an Artificial He
  - rank 17 → 451 | ml=0.005 bridge=0.812 | The GigaMIDI Dataset with Features for Expressive Music Perf
  - rank 16 → 470 | ml=0.003 bridge=0.796 | Improved Real-Time Six-Degrees-of-Freedom Dynamic Auralizati
  - rank 18 → 490 | ml=0.009 bridge=0.746 | Multimodal Datasets for Studying Expert Performances of Musi
  - rank 6 → 501 | ml=0.001 bridge=0.761 | ChoraleBricks: A Modular Multitrack Dataset for Wind Music R
  - rank 11 → 507 | ml=0.000 bridge=0.772 | BPSD: A Coherent Multi-Version Dataset for Analyzing the Fir
  - rank 2 → 513 | ml=0.000 bridge=0.758 | CCMusic: An Open and Diverse Database for Chinese Music Info
  - rank 5 → 516 | ml=0.006 bridge=0.672 | Towards an 'Everything Corpus': A Framework and Guidelines f

### Stable papers


## Worksheet

- Total worksheet rows: 60
- Bucket breakdown: {'promoted_by_hybrid': 20, 'demoted_by_hybrid': 20, 'high_ml_low_bridge_score': 10, 'high_bridge_score_low_ml': 10}

## Caveats

- This is not validation.
- ml_probability values come from a frozen full-fit model; they are in-sample for the 100 labeled rows and out-of-sample for the remaining 428.
- Rank percentiles are computed across all 528 Bridge candidates (full-pool pilot scope).
- bridge_score comes from rank-5a7efa5ca3 (cluster-enabled run); it was NULL in rank-83787b91ef.
- No API, UI, serving, or ranking-table changes are made or authorized.
- This shadow pilot is a pre-serving diagnostic only.
