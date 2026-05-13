# Source-split blind error analysis

Reviewer-facing offline audit of how frozen source-split learned logits reorder blind-source rows versus `final_score`.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **family / score family:** `emerging`
- **label_dataset_path:** `docs/audit/ml-label-dataset-v4.json`
- **label_dataset_sha256:** `88a3067b48f52b6a99295c51e75da54dd03b2b84bd43b9edc674755a28f92288`
- **source_split_artifact_path:** `docs/audit/ml-source-split-tiny-baseline-emerging-rank-ee2ba6c816-v4.json`
- **source_split_artifact_sha256:** `d018fdae5a921e8e69a4d1fd07640135d233fccf3699779486421d3e8dfa20e9`
- **conflict_policy_path:** `docs/audit/ml-label-conflict-policy.md`
- **conflict_policy_sha256:** `d0591de1a2bd9ab75c64f2318e9a5ea0b7acd94902e083931049893416d47841`

## Caveats

- Not validation.
- Blind-source offline diagnostic only.
- Learned model underperformed heuristic on source-split blind metrics for at least one target; must not drive production ranking.
- Buckets are for feature/label inspection, not product-quality claims.

## Feature Join

- **blind selected rows:** `60`
- **joined feature rows:** `60`
- **missing feature rows:** `0`

## Target `good_or_acceptable`

- **blind boolean rows:** `60`
- **class counts:** positive `51`, negative `9`
- **heuristic AUC:** `0.6776`
- **learned AUC:** `0.6667`

### Top-k Overlap

| k | heuristic size | learned size | intersection | union | Jaccard |
|---:|---:|---:|---:|---:|---:|
| 5 | 5 | 5 | 3 | 7 | 0.4286 |
| 10 | 10 | 10 | 9 | 11 | 0.8182 |
| 20 | 20 | 20 | 19 | 21 | 0.9048 |

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
| `https://openalex.org/W4415788031` | -27 | 0.2133 | 1.5453 | Music emotion clustering is relevant but narrow and modest |
| `https://openalex.org/W4409215246` | -10 | 0.4949 | 4.1294 | Audio filter numerical noise is relevant DSP but not discovery friendly |
| `https://openalex.org/W4406354762` | -4 | 0.4937 | 6.4766 | Spatial audio HRTF evaluation is useful audio technology research |
| `https://openalex.org/W4405710378` | -2 | 0.3609 | 3.2425 | Headphone ANC uncertainty modeling is useful but narrow audio engineering |
| `https://openalex.org/W4406354650` | -1 | 0.4504 | 4.1858 | Loudspeaker motor physics is relevant audio hardware but very narrow |

### Largest learned promotions

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W7128586572` | 6 | 0.4505 | 7.4616 | AR visual cues and binaural reproduction bridge spatial audio modalities |
| `https://openalex.org/W4409474113` | 3 | 0.4640 | 7.6055 | Diffusion restoration for historical music recordings is discovery friendly |
| `https://openalex.org/W4405710382` | 3 | 0.4456 | 5.1652 | Personal exposure at music events is useful but hearing safety focused |
| `https://openalex.org/W7154260406` | 2 | 0.1741 | 3.3445 | Cross-cultural MIR and NLP gaps are highly relevant and discovery friendly |
| `https://openalex.org/W7131131523` | 2 | 0.1737 | 3.2614 | Sentiment based music recommendation is relevant but conventional |

### Largest learned demotions

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W4415788031` | -27 | 0.2133 | 1.5453 | Music emotion clustering is relevant but narrow and modest |
| `https://openalex.org/W4409215246` | -10 | 0.4949 | 4.1294 | Audio filter numerical noise is relevant DSP but not discovery friendly |
| `https://openalex.org/W4406354762` | -4 | 0.4937 | 6.4766 | Spatial audio HRTF evaluation is useful audio technology research |
| `https://openalex.org/W4405710378` | -2 | 0.3609 | 3.2425 | Headphone ANC uncertainty modeling is useful but narrow audio engineering |
| `https://openalex.org/W4406354650` | -1 | 0.4504 | 4.1858 | Loudspeaker motor physics is relevant audio hardware but very narrow |

### Feature summaries by bucket

| bucket | feature | count | mean | median |
|---|---|---:|---:|---:|
| `promoted_positive` | `final_score` | 28 | 0.2306 | 0.1704 |
| `promoted_positive` | `semantic_score` | 28 | 0.8364 | 0.8403 |
| `promoted_positive` | `citation_velocity_score` | 28 | 0.0022 | 0.0000 |
| `promoted_positive` | `topic_growth_score` | 28 | 0.2075 | 0.0000 |
| `promoted_positive` | `diversity_penalty` | 28 | 0.0000 | 0.0000 |
| `promoted_negative` | `final_score` | 5 | 0.1681 | 0.1684 |
| `promoted_negative` | `semantic_score` | 5 | 0.8404 | 0.8420 |
| `promoted_negative` | `citation_velocity_score` | 5 | 0.0000 | 0.0000 |
| `promoted_negative` | `topic_growth_score` | 5 | 0.0000 | 0.0000 |
| `promoted_negative` | `diversity_penalty` | 5 | 0.0000 | 0.0000 |
| `demoted_positive` | `final_score` | 5 | 0.4026 | 0.4504 |
| `demoted_positive` | `semantic_score` | 5 | 0.7974 | 0.8124 |
| `demoted_positive` | `citation_velocity_score` | 5 | 0.0970 | 0.0909 |
| `demoted_positive` | `topic_growth_score` | 5 | 0.6488 | 0.8333 |
| `demoted_positive` | `diversity_penalty` | 5 | 0.0000 | 0.0000 |
| `demoted_negative` | `final_score` | 0 | n/a | n/a |
| `demoted_negative` | `semantic_score` | 0 | n/a | n/a |
| `demoted_negative` | `citation_velocity_score` | 0 | n/a | n/a |
| `demoted_negative` | `topic_growth_score` | 0 | n/a | n/a |
| `demoted_negative` | `diversity_penalty` | 0 | n/a | n/a |

### Interpretation

These rows describe correlations between frozen learned-logit reorderings, labels, and persisted features inside the blind-source slice. They do not establish causation or product-quality impact; labels are single-reviewer judgments and the blind sample still has sampling design constraints.

## Target `surprising_or_useful`

- **blind boolean rows:** `60`
- **class counts:** positive `46`, negative `14`
- **heuristic AUC:** `0.6009`
- **learned AUC:** `0.4798`

### Top-k Overlap

| k | heuristic size | learned size | intersection | union | Jaccard |
|---:|---:|---:|---:|---:|---:|
| 5 | 5 | 5 | 2 | 8 | 0.2500 |
| 10 | 10 | 10 | 3 | 17 | 0.1765 |
| 20 | 20 | 20 | 13 | 27 | 0.4815 |

### Promoted negatives

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W7124548874` | 10 | 0.1729 | 2.1658 | Emotion based recommendation for wellness is relevant but predictable |
| `https://openalex.org/W7131131523` | 10 | 0.1737 | 2.2895 | Sentiment based music recommendation is relevant but conventional |
| `https://openalex.org/W7128805121` | 10 | 0.1720 | 2.0220 | AI music review is relevant overview but broad and predictable |
| `https://openalex.org/W7154583761` | 7 | 0.1696 | 1.6474 | Urban social noise classification is audio adjacent but off product target |
| `https://openalex.org/W7142281766` | 7 | 0.1684 | 1.4633 | Background music in classrooms is music adjacent education research |

### Demoted positives

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W4409215246` | -56 | 0.4949 | -0.0610 | Audio filter numerical noise is relevant DSP but not discovery friendly |
| `https://openalex.org/W4406354730` | -48 | 0.4476 | -0.2483 | Flat panel loudspeaker exciter modeling is relevant but very hardware narrow |
| `https://openalex.org/W4406354650` | -41 | 0.4504 | 0.5341 | Loudspeaker motor physics is relevant audio hardware but very narrow |
| `https://openalex.org/W4411141387` | -28 | 0.4556 | 0.9979 | Digital audio effects antialiasing is relevant DSP for music technology |
| `https://openalex.org/W4405710382` | -21 | 0.4456 | 1.1114 | Personal exposure at music events is useful but hearing safety focused |

### Largest learned promotions

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W7154260406` | 11 | 0.1741 | 2.3535 | Cross-cultural MIR and NLP gaps are highly relevant and discovery friendly |
| `https://openalex.org/W7108084681` | 10 | 0.1720 | 2.0187 | Regional Turkish folk audio classification is useful underrepresented MIR |
| `https://openalex.org/W7124548874` | 10 | 0.1729 | 2.1658 | Emotion based recommendation for wellness is relevant but predictable |
| `https://openalex.org/W7131354888` | 10 | 0.1731 | 2.1931 | U-Net source separation analysis is core but not especially unexpected |
| `https://openalex.org/W4415242171` | 10 | 0.1725 | 2.0906 | Mamba based music source separation is core and timely MIR |

### Largest learned demotions

| paper_id | rank_delta | final_score | learned_logit | reviewer notes |
|---|---:|---:|---:|---|
| `https://openalex.org/W4409215246` | -56 | 0.4949 | -0.0610 | Audio filter numerical noise is relevant DSP but not discovery friendly |
| `https://openalex.org/W4406354730` | -48 | 0.4476 | -0.2483 | Flat panel loudspeaker exciter modeling is relevant but very hardware narrow |
| `https://openalex.org/W4406354650` | -41 | 0.4504 | 0.5341 | Loudspeaker motor physics is relevant audio hardware but very narrow |
| `https://openalex.org/W4411141387` | -28 | 0.4556 | 0.9979 | Digital audio effects antialiasing is relevant DSP for music technology |
| `https://openalex.org/W4405710382` | -21 | 0.4456 | 1.1114 | Personal exposure at music events is useful but hearing safety focused |

### Feature summaries by bucket

| bucket | feature | count | mean | median |
|---|---|---:|---:|---:|
| `promoted_positive` | `final_score` | 34 | 0.1839 | 0.1664 |
| `promoted_positive` | `semantic_score` | 34 | 0.8308 | 0.8294 |
| `promoted_positive` | `citation_velocity_score` | 34 | 0.0027 | 0.0000 |
| `promoted_positive` | `topic_growth_score` | 34 | 0.0548 | 0.0000 |
| `promoted_positive` | `diversity_penalty` | 34 | 0.0000 | 0.0000 |
| `promoted_negative` | `final_score` | 13 | 0.1675 | 0.1674 |
| `promoted_negative` | `semantic_score` | 13 | 0.8377 | 0.8369 |
| `promoted_negative` | `citation_velocity_score` | 13 | 0.0000 | 0.0000 |
| `promoted_negative` | `topic_growth_score` | 13 | 0.0000 | 0.0000 |
| `promoted_negative` | `diversity_penalty` | 13 | 0.0000 | 0.0000 |
| `demoted_positive` | `final_score` | 10 | 0.4284 | 0.4530 |
| `demoted_positive` | `semantic_score` | 10 | 0.7904 | 0.7954 |
| `demoted_positive` | `citation_velocity_score` | 10 | 0.0545 | 0.0758 |
| `demoted_positive` | `topic_growth_score` | 10 | 0.8101 | 0.9762 |
| `demoted_positive` | `diversity_penalty` | 10 | 0.0000 | 0.0000 |
| `demoted_negative` | `final_score` | 0 | n/a | n/a |
| `demoted_negative` | `semantic_score` | 0 | n/a | n/a |
| `demoted_negative` | `citation_velocity_score` | 0 | n/a | n/a |
| `demoted_negative` | `topic_growth_score` | 0 | n/a | n/a |
| `demoted_negative` | `diversity_penalty` | 0 | n/a | n/a |

### Interpretation

These rows describe correlations between frozen learned-logit reorderings, labels, and persisted features inside the blind-source slice. They do not establish causation or product-quality impact; labels are single-reviewer judgments and the blind sample still has sampling design constraints.
