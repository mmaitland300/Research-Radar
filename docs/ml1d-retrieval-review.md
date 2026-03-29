# ML1d Retrieval Review

Worksheet for a **lightweight qualitative** pass on paper-to-paper neighbors before using retrieval in ranking. Use **5 anchors**, **top 5 neighbors** each (paper detail Similar block or `GET /api/v1/papers/{id}/similar`). **Embedding quality is tied to corpus state**, not `embedding_version` alone—record the snapshot you reviewed against.

## Column reference (conceptual)

| Field | Purpose |
| --- | --- |
| `anchor_paper_id` | OpenAlex URL id of the anchor (outside table, below) |
| `anchor_title` | Short reference (outside table, below) |
| `neighbor_rank` | 1–5 |
| `neighbor_title` | As in API/UI |
| `similarity` | From API (`1 - cosine_distance`) |
| `judgment` | Per-neighbor: `good` / `mixed` / `weak` (see rubric) |
| `notes` | Optional tags from legend + short free text |

Neighbor `paper_id` does not need to be in the table unless you want it for traceability.

---

## Run metadata

**Embedding version:** `v1-title-abstract-1536`  
**Corpus snapshot version:** `source-snapshot-...`  
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

**anchor_paper_id:**  
**anchor_title:**

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 |  |  | good / mixed / weak |  |
| 2 |  |  | good / mixed / weak |  |
| 3 |  |  | good / mixed / weak |  |
| 4 |  |  | good / mixed / weak |  |
| 5 |  |  | good / mixed / weak |  |

**Summary**

- overall:
- main failure mode:
- demo_worthy: yes / no *(good enough to showcase, not just “technically working”)*

---

## Anchor 2

**anchor_paper_id:**  
**anchor_title:**

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 |  |  | good / mixed / weak |  |
| 2 |  |  | good / mixed / weak |  |
| 3 |  |  | good / mixed / weak |  |
| 4 |  |  | good / mixed / weak |  |
| 5 |  |  | good / mixed / weak |  |

**Summary**

- overall:
- main failure mode:
- demo_worthy: yes / no

---

## Anchor 3

**anchor_paper_id:**  
**anchor_title:**

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 |  |  | good / mixed / weak |  |
| 2 |  |  | good / mixed / weak |  |
| 3 |  |  | good / mixed / weak |  |
| 4 |  |  | good / mixed / weak |  |
| 5 |  |  | good / mixed / weak |  |

**Summary**

- overall:
- main failure mode:
- demo_worthy: yes / no

---

## Anchor 4

**anchor_paper_id:**  
**anchor_title:**

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 |  |  | good / mixed / weak |  |
| 2 |  |  | good / mixed / weak |  |
| 3 |  |  | good / mixed / weak |  |
| 4 |  |  | good / mixed / weak |  |
| 5 |  |  | good / mixed / weak |  |

**Summary**

- overall:
- main failure mode:
- demo_worthy: yes / no

---

## Anchor 5

**anchor_paper_id:**  
**anchor_title:**

**Neighbors**

| rank | neighbor_title | similarity | judgment | notes |
| --- | --- | ---: | --- | --- |
| 1 |  |  | good / mixed / weak |  |
| 2 |  |  | good / mixed / weak |  |
| 3 |  |  | good / mixed / weak |  |
| 4 |  |  | good / mixed / weak |  |
| 5 |  |  | good / mixed / weak |  |

**Summary**

- overall:
- main failure mode:
- demo_worthy: yes / no

---

## Roll-up

**Per-anchor set verdict** (one line each: anchor id + good/mixed/weak for the set as a whole):

1.
2.
3.
4.
5.

**Counts**

- `good_sets`: X/5
- `mixed_sets`: X/5
- `weak_sets`: X/5

**common_failure_modes:**

**recommended_next_step:**  
*(e.g. `clean text encoding before ranking integration` / `safe to prototype semantic ranking feature` / `needs broader embedding coverage first`)*

**go/no-go for ranking integration:**
