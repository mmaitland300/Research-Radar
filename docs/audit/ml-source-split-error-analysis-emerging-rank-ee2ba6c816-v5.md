# Source-split blind error analysis

Reviewer-facing offline audit of how frozen source-split learned logits reorder blind-source rows versus `final_score`.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **family / score family:** `emerging`
- **label_dataset_path:** `docs/audit/ml-label-dataset-v5.json`
- **label_dataset_sha256:** `0dde7a62d4e7d628aa7626f5501c0982603188e1542bc381ec054e615b1ff6d7`
- **source_split_artifact_path:** `docs/audit/ml-source-split-tiny-baseline-emerging-rank-ee2ba6c816-v5.json`
- **source_split_artifact_sha256:** `2a023e2ce717b7f1ba595197005a2f4219385a848c926f2a0cec90d488ae2732`
- **conflict_policy_path:** `docs/audit/ml-label-conflict-policy.md`
- **conflict_policy_sha256:** `d0591de1a2bd9ab75c64f2318e9a5ea0b7acd94902e083931049893416d47841`

## Caveats

- Not validation.
- Blind-source offline diagnostic only.
- Learned model underperformed heuristic on source-split blind metrics for at least one target; must not drive production ranking.
- Buckets are for feature/label inspection, not product-quality claims.

## Feature Join

- **blind selected rows:** `120`
- **joined feature rows:** `120`
- **missing feature rows:** `0`

## Target `good_or_acceptable`

- **blind boolean rows:** `120`
- **class counts:** positive `110`, negative `10`
- **heuristic AUC:** `0.7355`
- **learned AUC:** `0.7309`

### Top-k Overlap

| k | heuristic size | learned size | intersection | union | Jaccard |
|---:|---:|---:|---:|---:|---:|
| 5 | 5 | 5 | 2 | 8 | 0.2500 |
| 10 | 10 | 10 | 7 | 13 | 0.5385 |
| 20 | 20 | 20 | 18 | 22 | 0.8182 |

### Promoted negatives

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W7154583761` | 1 | 0.1696 | 2.4266 | Urban social noise classification is audio adjacent but off product target |
| `https://openalex.org/W7142281766` | 1 | 0.1684 | 2.1873 | Background music in classrooms is music adjacent education research |
| `https://openalex.org/W7155372642` | 1 | 0.1674 | 1.9710 | Animal sound detection is audio ML but not music technology discovery |
| `https://openalex.org/W7140166340` | 1 | 0.1666 | 1.8026 | Lecture recording retrieval is outside music and audio technology discovery |
| `https://openalex.org/W7143753957` | 1 | 0.1684 | 2.1785 | Sports assessment uses music synchrony but recommendation target is off slice |

### Demoted positives

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W4415788031` | -60 | 0.2133 | 1.5453 | Music emotion clustering is relevant but narrow and modest |
| `https://openalex.org/W4409215246` | -20 | 0.4949 | 4.1294 | Audio filter numerical noise is relevant DSP but not discovery friendly |
| `https://openalex.org/W4415584170` | -19 | 0.2192 | 2.7501 | Non-Western timbre-aware MSS; clear MIR contribution addressing dataset and model bias... |
| `https://openalex.org/W4414012206` | -12 | 0.4955 | 6.5177 | Headrest crosstalk cancellation for car-like binaural playback; applied spatial audio e... |
| `https://openalex.org/W4406354762` | -11 | 0.4937 | 6.4766 | Spatial audio HRTF evaluation is useful audio technology research |

### Largest learned promotions

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W4414799828` | 17 | 0.4398 | 7.5608 | Psychoacoustic listening-test design for personal sound zones; core spatial audio and Q... |
| `https://openalex.org/W4414799851` | 14 | 0.4529 | 7.6142 | Multitask DNN for joint source localization and room geometry from audio; solid spatial... |
| `https://openalex.org/W7128586572` | 12 | 0.4505 | 7.4616 | AR visual cues and binaural reproduction bridge spatial audio modalities |
| `https://openalex.org/W4409474087` | 6 | 0.4333 | 6.2434 | EV truck AVAS / warning sound design; transport psychoacoustics with limited overlap to... |
| `https://openalex.org/W4410025357` | 4 | 0.4304 | 5.6501 | Listening-test methodology crossing lighting and audio perception; peripheral to core M... |

### Largest learned demotions

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W4415788031` | -60 | 0.2133 | 1.5453 | Music emotion clustering is relevant but narrow and modest |
| `https://openalex.org/W4409215246` | -20 | 0.4949 | 4.1294 | Audio filter numerical noise is relevant DSP but not discovery friendly |
| `https://openalex.org/W4415584170` | -19 | 0.2192 | 2.7501 | Non-Western timbre-aware MSS; clear MIR contribution addressing dataset and model bias... |
| `https://openalex.org/W4414012206` | -12 | 0.4955 | 6.5177 | Headrest crosstalk cancellation for car-like binaural playback; applied spatial audio e... |
| `https://openalex.org/W4406354762` | -11 | 0.4937 | 6.4766 | Spatial audio HRTF evaluation is useful audio technology research |

### Feature summaries by bucket

| bucket | feature | count | mean | median |
|---|---|---:|---:|---:|
| `promoted_positive` | `final_score` | 61 | 0.1969 | 0.1698 |
| `promoted_positive` | `semantic_score` | 61 | 0.8458 | 0.8449 |
| `promoted_positive` | `citation_velocity_score` | 61 | 0.0000 | 0.0000 |
| `promoted_positive` | `topic_growth_score` | 61 | 0.0925 | 0.0000 |
| `promoted_positive` | `diversity_penalty` | 61 | 0.0000 | 0.0000 |
| `promoted_negative` | `final_score` | 5 | 0.1681 | 0.1684 |
| `promoted_negative` | `semantic_score` | 5 | 0.8404 | 0.8420 |
| `promoted_negative` | `citation_velocity_score` | 5 | 0.0000 | 0.0000 |
| `promoted_negative` | `topic_growth_score` | 5 | 0.0000 | 0.0000 |
| `promoted_negative` | `diversity_penalty` | 5 | 0.0000 | 0.0000 |
| `demoted_positive` | `final_score` | 11 | 0.4156 | 0.4551 |
| `demoted_positive` | `semantic_score` | 11 | 0.7997 | 0.8124 |
| `demoted_positive` | `citation_velocity_score` | 11 | 0.0689 | 0.0909 |
| `demoted_positive` | `topic_growth_score` | 11 | 0.7376 | 0.9524 |
| `demoted_positive` | `diversity_penalty` | 11 | 0.0000 | 0.0000 |
| `demoted_negative` | `final_score` | 0 | n/a | n/a |
| `demoted_negative` | `semantic_score` | 0 | n/a | n/a |
| `demoted_negative` | `citation_velocity_score` | 0 | n/a | n/a |
| `demoted_negative` | `topic_growth_score` | 0 | n/a | n/a |
| `demoted_negative` | `diversity_penalty` | 0 | n/a | n/a |

### Interpretation

These rows describe correlations between frozen learned-logit reorderings, labels, and persisted features inside the blind-source slice. They do not establish causation or product-quality impact; labels are single-reviewer judgments and the blind sample still has sampling design constraints.

## Target `surprising_or_useful`

- **blind boolean rows:** `120`
- **class counts:** positive `101`, negative `19`
- **heuristic AUC:** `0.6175`
- **learned AUC:** `0.5331`

### Top-k Overlap

| k | heuristic size | learned size | intersection | union | Jaccard |
|---:|---:|---:|---:|---:|---:|
| 5 | 5 | 5 | 1 | 9 | 0.1111 |
| 10 | 10 | 10 | 2 | 18 | 0.1111 |
| 20 | 20 | 20 | 5 | 35 | 0.1429 |

### Promoted negatives

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W7133595965` | 20 | 0.1734 | 2.2354 | DRL for teaching interaction reads like incremental EdTech + MIR; relevance OK but nove... |
| `https://openalex.org/W7131131523` | 20 | 0.1737 | 2.2895 | Sentiment based music recommendation is relevant but conventional |
| `https://openalex.org/W7124548874` | 19 | 0.1729 | 2.1658 | Emotion based recommendation for wellness is relevant but predictable |
| `https://openalex.org/W7128805121` | 19 | 0.1720 | 2.0220 | AI music review is relevant overview but broad and predictable |
| `https://openalex.org/W7154583761` | 10 | 0.1696 | 1.6474 | Urban social noise classification is audio adjacent but off product target |

### Demoted positives

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W4409215246` | -113 | 0.4949 | -0.0610 | Audio filter numerical noise is relevant DSP but not discovery friendly |
| `https://openalex.org/W4406354730` | -98 | 0.4476 | -0.2483 | Flat panel loudspeaker exciter modeling is relevant but very hardware narrow |
| `https://openalex.org/W4406354650` | -86 | 0.4504 | 0.5341 | Loudspeaker motor physics is relevant audio hardware but very narrow |
| `https://openalex.org/W4411141320` | -71 | 0.4551 | 0.9253 | Virtual analog and WDF plug-in generation; strong audio DSP angle with limited cross-do... |
| `https://openalex.org/W4411141387` | -66 | 0.4556 | 0.9979 | Digital audio effects antialiasing is relevant DSP for music technology |

### Largest learned promotions

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W7154260406` | 21 | 0.1741 | 2.3535 | Cross-cultural MIR and NLP gaps are highly relevant and discovery friendly |
| `https://openalex.org/W7107871673` | 20 | 0.1738 | 2.2948 | Graph-based symbolic boundary detection and changepoint methods; solid MSA contribution... |
| `https://openalex.org/W7104398342` | 20 | 0.1732 | 2.2093 | Relevant OMR paper combining modern vision architectures for complex polyphonic score t... |
| `https://openalex.org/W7131354888` | 20 | 0.1731 | 2.1931 | U-Net source separation analysis is core but not especially unexpected |
| `https://openalex.org/W7133595965` | 20 | 0.1734 | 2.2354 | DRL for teaching interaction reads like incremental EdTech + MIR; relevance OK but nove... |

### Largest learned demotions

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W4409215246` | -113 | 0.4949 | -0.0610 | Audio filter numerical noise is relevant DSP but not discovery friendly |
| `https://openalex.org/W4406354730` | -98 | 0.4476 | -0.2483 | Flat panel loudspeaker exciter modeling is relevant but very hardware narrow |
| `https://openalex.org/W4406354650` | -86 | 0.4504 | 0.5341 | Loudspeaker motor physics is relevant audio hardware but very narrow |
| `https://openalex.org/W4411141320` | -71 | 0.4551 | 0.9253 | Virtual analog and WDF plug-in generation; strong audio DSP angle with limited cross-do... |
| `https://openalex.org/W4411141387` | -66 | 0.4556 | 0.9979 | Digital audio effects antialiasing is relevant DSP for music technology |

### Feature summaries by bucket

| bucket | feature | count | mean | median |
|---|---|---:|---:|---:|
| `promoted_positive` | `final_score` | 79 | 0.1825 | 0.1680 |
| `promoted_positive` | `semantic_score` | 79 | 0.8357 | 0.8378 |
| `promoted_positive` | `citation_velocity_score` | 79 | 0.0023 | 0.0000 |
| `promoted_positive` | `topic_growth_score` | 79 | 0.0472 | 0.0000 |
| `promoted_positive` | `diversity_penalty` | 79 | 0.0000 | 0.0000 |
| `promoted_negative` | `final_score` | 17 | 0.1679 | 0.1684 |
| `promoted_negative` | `semantic_score` | 17 | 0.8397 | 0.8420 |
| `promoted_negative` | `citation_velocity_score` | 17 | 0.0000 | 0.0000 |
| `promoted_negative` | `topic_growth_score` | 17 | 0.0000 | 0.0000 |
| `promoted_negative` | `diversity_penalty` | 17 | 0.0000 | 0.0000 |
| `demoted_positive` | `final_score` | 20 | 0.4433 | 0.4600 |
| `demoted_positive` | `semantic_score` | 20 | 0.7960 | 0.8022 |
| `demoted_positive` | `citation_velocity_score` | 20 | 0.0318 | 0.0000 |
| `demoted_positive` | `topic_growth_score` | 20 | 0.8940 | 1.0000 |
| `demoted_positive` | `diversity_penalty` | 20 | 0.0000 | 0.0000 |
| `demoted_negative` | `final_score` | 0 | n/a | n/a |
| `demoted_negative` | `semantic_score` | 0 | n/a | n/a |
| `demoted_negative` | `citation_velocity_score` | 0 | n/a | n/a |
| `demoted_negative` | `topic_growth_score` | 0 | n/a | n/a |
| `demoted_negative` | `diversity_penalty` | 0 | n/a | n/a |

### Interpretation

These rows describe correlations between frozen learned-logit reorderings, labels, and persisted features inside the blind-source slice. They do not establish causation or product-quality impact; labels are single-reviewer judgments and the blind sample still has sampling design constraints.
