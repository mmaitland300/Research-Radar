# ML1d Retrieval Review

Worksheet for a lightweight qualitative pass on paper-to-paper neighbors before using retrieval in ranking. Use 5 anchors, top 5 neighbors each (paper detail Similar block or `GET /api/v1/papers/{id}/similar`). Embedding quality is tied to corpus state, not `embedding_version` alone, so record the snapshot you reviewed against.

Goal: judge whether embedding neighbors are good enough for product demo use and early ranking experiments.

## Column reference (conceptual)

| Field | Purpose |
| --- | --- |
| `anchor_paper_id` | OpenAlex URL id of the anchor (outside table, below) |
| `anchor_title` | Short reference (outside table, below) |
| `neighbor_rank` | 1-5 |
| `neighbor_title` | As in API/UI |
| `similarity` | From API (`1 - cosine_distance`) |
| `judgment` | Per-neighbor: `good` / `mixed` / `weak` (see rubric) |
| `notes` | Optional tags from legend + short free text |

Default neighbor table has no `paper_id` column. If you need an id for traceability, put it in `notes`. Consistency beats extra columns at this stage.

---

## Run metadata

**Embedding version:** `v1-title-abstract-1536`  
**Corpus snapshot version:** `source-snapshot-20260328-170751`  
**Date:** `2026-03-29`

**Judgment rubric**

- **good:** Most neighbors feel semantically aligned with the anchor.
- **mixed:** Some useful neighbors, some noisy ones.
- **weak:** Mostly venue / topic / title noise (or otherwise not useful as a set).

**Note tags** (use in `notes` column or roll-up)

- `same-venue-bias`
- `dataset-title-bias`
- `too-broad`
- `metadata-noise`
- `encoding-issue`
- `strong-match`

---

## Anchor 1

**anchor_paper_id:** `https://openalex.org/W7119099299`  
**anchor_title:** `Supervised Contrastive Models for Music Information Retrieval in Classical Persian Music`

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 | CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research | 0.6127 | mixed | `strong-match`; same MIR + culturally specific music focus, but dataset-heavy |
| 2 | The GigaMIDI Dataset with Features for Expressive Music Performance Detection | 0.5900 | mixed | `dataset-title-bias`; music modeling / performance signal overlap |
| 3 | Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data | 0.5645 | weak | `too-broad`; corpus-crafting rather than supervised contrastive MIR |
| 4 | MusiQAl: A Dataset for Music Question&amp;ndash;Answering through Audio&amp;ndash;Video Fusion | 0.5617 | weak | `dataset-title-bias`; broad multimodal music data rather than retrieval modeling |
| 5 | ChoraleBricks: A Modular Multitrack Dataset for Wind Music Research | 0.5226 | weak | `dataset-title-bias`; same broad domain, weak task alignment |

**Summary**

- overall: mixed
- main_failure_mode: dataset-title-bias
- demo_worthy: no - retrieval is plausibly in-domain, but not tight enough to showcase semantic precision yet.

---

## Anchor 2

**anchor_paper_id:** `https://openalex.org/W7128803784`  
**anchor_title:** `RWC Revisited: Towards a Community-Driven MIR Corpus`

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 | Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data | 0.6427 | good | `strong-match`; strong corpus curation / community-resource alignment |
| 2 | CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research | 0.6359 | good | `strong-match`; another MIR dataset / corpus-building paper |
| 3 | Beyond a Western Center of Music Information Retrieval: A Bibliometric Analysis of the First 25 Years of ISMIR Authorship | 0.6045 | mixed | community / field-structure overlap, but not corpus-focused |
| 4 | ChoraleBricks: A Modular Multitrack Dataset for Wind Music Research | 0.5685 | good | `strong-match`; dataset / resource paper in the same slice |
| 5 | MGPHot: A Dataset of Musicological Annotations for Popular Music (1958&amp;ndash;2022) | 0.5521 | good | `strong-match`; dataset / annotation resource, close product-use category |

**Summary**

- overall: good
- main_failure_mode: same-venue-bias
- demo_worthy: yes - this is a solid retrieval example for corpus / dataset papers in MIR.

---

## Anchor 3

**anchor_paper_id:** `https://openalex.org/W4415947443`  
**anchor_title:** `Beyond a Western Center of Music Information Retrieval: A Bibliometric Analysis of the First 25 Years of ISMIR Authorship`

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 | Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data | 0.6156 | mixed | broad field / corpus discourse overlap, but not bibliometric |
| 2 | RWC Revisited: Towards a Community-Driven MIR Corpus | 0.6045 | mixed | MIR community overlap, but corpus resource rather than authorship analysis |
| 3 | CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research | 0.6013 | mixed | non-Western MIR context helps, but still mostly dataset-level |
| 4 | MGPHot: A Dataset of Musicological Annotations for Popular Music (1958&amp;ndash;2022) | 0.5161 | weak | `dataset-title-bias`; little bibliometric or authorship-analysis alignment |
| 5 | Supervised Contrastive Models for Music Information Retrieval in Classical Persian Music | 0.5050 | weak | `too-broad`; only shared MIR / culturally specific music framing |

**Summary**

- overall: weak
- main_failure_mode: too-broad
- demo_worthy: no - retrieval stays inside MIR, but the neighbors do not preserve the bibliometric / authorship-analysis intent.

---

## Anchor 4

**anchor_paper_id:** `https://openalex.org/W4413990340`  
**anchor_title:** `Smartwatch-Based Audio&amp;ndash;Gestural Insights in Violin Bow Stroke Analyses`

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 | The GigaMIDI Dataset with Features for Expressive Music Performance Detection | 0.5571 | good | `strong-match`; performance-expression angle is meaningfully related |
| 2 | Supervised Contrastive Models for Music Information Retrieval in Classical Persian Music | 0.5111 | mixed | modeling overlap, but task and modality differ |
| 3 | MusiQAl: A Dataset for Music Question&amp;ndash;Answering through Audio&amp;ndash;Video Fusion | 0.5029 | mixed | multimodal audio context helps, but not gestural violin analysis |
| 4 | ChoraleBricks: A Modular Multitrack Dataset for Wind Music Research | 0.4703 | mixed | performance / music-analysis adjacency, but weak task match |
| 5 | Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data | 0.4248 | weak | `too-broad`; corpus paper rather than instrument-gesture analysis |

**Summary**

- overall: mixed
- main_failure_mode: too-broad
- demo_worthy: no - top neighbor is promising, but the rest of the set drifts quickly into broad music-data adjacency.

---

## Anchor 5

**anchor_paper_id:** `https://openalex.org/W4412780451`  
**anchor_title:** `MusiQAl: A Dataset for Music Question&amp;ndash;Answering through Audio&amp;ndash;Video Fusion`

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 | Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data | 0.5974 | good | `strong-match`; multimodal corpus framing is highly relevant |
| 2 | CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research | 0.5926 | mixed | music dataset proximity, but not QA / AV fusion |
| 3 | The GigaMIDI Dataset with Features for Expressive Music Performance Detection | 0.5671 | mixed | dataset-title-bias; music data adjacency more than task similarity |
| 4 | Supervised Contrastive Models for Music Information Retrieval in Classical Persian Music | 0.5617 | mixed | MIR modeling adjacency, but no QA or AV fusion angle |
| 5 | MGPHot: A Dataset of Musicological Annotations for Popular Music (1958&amp;ndash;2022) | 0.5387 | mixed | annotation / dataset overlap, but broad |

**Summary**

- overall: mixed
- main_failure_mode: dataset-title-bias
- demo_worthy: no - the first neighbor is strong, but the set as a whole is more 'music dataset neighborhood' than task-specific retrieval.

---

## Roll-up

**Per-anchor set verdict**

1. Anchor 1 - mixed
2. Anchor 2 - good
3. Anchor 3 - weak
4. Anchor 4 - mixed
5. Anchor 5 - mixed

**Counts**

- `good_sets`: 1/5 (Anchors 2)
- `mixed_sets`: 3/5 (Anchors 1, 4, 5)
- `weak_sets`: 1/5 (Anchors 3)

**common_failure_modes:** `dataset-title-bias`, `too-broad`, `same-venue-bias`, `encoding-issue`

**recommended_next_step** (pick one best next engineering move): clean text encoding before ranking integration (or `safe to prototype semantic ranking feature`, or `needs broader embedding coverage first`)

**go/no-go for ranking integration:** No-go for semantic ranking integration yet. Retrieval is good enough to demo as an exploratory similar-papers surface, but not yet stable enough to feed ranking without more coverage and cleanup.
