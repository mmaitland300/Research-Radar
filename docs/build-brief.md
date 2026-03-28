# Build Brief

## Name

Research Radar: Emerging and Bridge Papers in Audio ML

## Thesis

Find emerging and bridge papers in audio ML before they become default citations.

## Corpus slice

V1 focuses on:

- music information retrieval
- audio representation learning

V1.1 controlled edge slice:

- neural audio effects
- music/audio generation only when papers citation-connect back to the core corpus

## Inclusion rules

A work is included in the V1 ranking corpus only if it passes all of the following:

### Document type

- proceedings article
- journal article
- preprint

Excluded:

- editorials
- posters without abstracts
- tutorials
- patents
- books

### Language

- English only

### Time range

- ranking candidates: 2016 to present
- optional citation context only: pre-2016

### Core-source allowlist

- ISMIR proceedings
- TISMIR
- DAFx proceedings
- JAES
- ICASSP papers that also pass the topic gate

### Topic gate

The work must match at least one strong signal in title, abstract, or topic metadata:

- music information retrieval
- audio representation learning
- music tagging
- music transcription
- source separation
- beat tracking
- onset detection
- music similarity
- MIR evaluation
- self-supervised audio
- contrastive audio
- audio embeddings

### Explicit exclusions

- pure speech recognition
- speaker verification
- general acoustics with no music/audio-ML relevance
- biomedical audio unless strongly connected to the core slice
- generic diffusion or audio-generation work with no music-analysis link

### Controlled edge-slice inclusion

Allow a paper from outside the core venue list only if:

- it matches the topic gate, and
- it is citation-connected to the core corpus above a threshold, or
- it is manually approved into a curated allowlist

## Product pages

V1 ships only:

- Search
- Recommended
- Paper Detail
- Trends
- Evaluation

No graph page in V1.

## Ranking formula

`final = w_s*S + w_c*C + w_t*T + w_b*B - w_d*D`

Signals:

- `S`: semantic relevance
- `C`: citation velocity
- `T`: local topic growth
- `B`: bridge score
- `D`: diversity penalty

Initial weights:

- `w_s = 0.30`
- `w_c = 0.20`
- `w_t = 0.20`
- `w_b = 0.20`
- `w_d = 0.10`

## Evaluation protocol

MVP is incomplete without all three:

1. Hand-reviewed relevance benchmark
2. Novelty and diversity comparison against citation-only baselines
3. Temporal backtest with a freeze-at-T evaluation

## Reproducibility

Every major output is versioned:

- `corpus_snapshot_version`
- `embedding_version`
- `ranking_version`

## Strategic note

The V1 edge slice for `neural audio effects` is deferred to `V1.1` unless a paper is clearly bridge-worthy into the MIR and audio-representation core. This keeps the initial corpus coherent while preserving room for bridge-paper logic later.
