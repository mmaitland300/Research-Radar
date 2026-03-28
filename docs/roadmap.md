# Roadmap

This document is the **implementation sequence** for V1: foundation → ranking infrastructure → ranked product → trends and evaluation → semantic refinement. It separates **ranking plumbing** (durable, honest) from **signal quality** (evolves over time).

## Where things stand

- **Bootstrap / ingest:** Corpus policy, OpenAlex bootstrap, Postgres schema, raw retention, and manifest counts are in place for the current slice.
- **Live product slice:** DB-backed search and paper detail; topic metadata flows through normalize + `work_topics`; list/detail IDs support full OpenAlex URLs where needed.
- **Not yet product-complete:** Materialized `paper_scores` writes, ranked recommendations API, corpus-scoped trends, evaluation vs baselines, embeddings-backed semantic and bridge signals.

---

## Recommended build sequence

### 1. Metadata contracts + UX foundation

**Goal:** Make ingest visible in the UI and stabilize paper/topic contracts before ranking and clients depend on them.

**Deliverables**

- Extend the list API so each item includes **top 2–3 topic names** per paper, using **one SQL shape** (aggregation / join / subquery)—no N+1 per row.
- **Search:** topic chips on `apps/web/app/search/page.tsx`.
- **Paper detail:** topic chips, clearer **source** block, optional **source display name** on the detail contract if missing from the API/repo query.
- Skip `GET /api/v1/topics` until autocomplete work actually starts.

**Why first:** Proves topic data in-product; hardens list/detail contracts (including identifier and field stability) before ranking layers consume them.

---

### 2. Ranking data model and run plumbing

**Goal:** Durable infrastructure so Research Radar is a **ranking product**, not a browser—without pretending stub signals are final science.

**Deliverables**

- **Write path** for `ranking_runs`: insert rows with `ranking_version`, `corpus_snapshot_version`, `embedding_version`, `notes`.
- **Write path** for `paper_scores`: persist `ranking_version`, `recommendation_family`, `final_score`, per-signal columns, and **foreign keys** so every score row ties to corpus state (`corpus_snapshot_version` via `ranking_runs`).
- **Contract clarity:** document which fields are “real v0” vs “placeholder / not yet modeled” in code or release notes—**do not** present silent zeros as authoritative semantic or bridge scores.

**Why before fancy scoring:** Tables and runs are the backbone; heuristics can evolve without reshaping the product architecture.

---

### 3. Ranking v0 (honest heuristic pass)

**Goal:** First materialized ranked outputs for all three families: **emerging**, **bridge**, **undercited**.

**Signal policy**

- **Use real proxies where possible:** citation velocity, recency, topic prevalence or simple growth proxies from curated corpus + `work_topics`.
- **Be explicit where not modeled:** semantic relevance and bridge score may be **null**, omitted in API, or labeled **not_yet_modeled** until embeddings/clusters exist—prefer honesty over fake richness.
- **Version naming:** e.g. `v0-heuristic-no-embeddings` so consumers know what they are getting.

---

### 4. Ranked recommendations API + explanation contract

**Goal:** Expose the core differentiator: **what to pay attention to now, and why**.

**Deliverables**

- New API module for **read path** (join `paper_scores` → `works`, optional topics).
- **Single endpoint style:** `GET /api/v1/recommendations/ranked?family=emerging|bridge|undercited&limit=…` (optional `ranking_version`; default = latest for current snapshot).
- **Response fields:** `paper_id`, `title`, `year`, `citation_count`, `source_slug`, `topics`, **per-signal map** (with explicit handling for not-yet-modeled signals), `final_score`, `reason_short`, `ranking_version`, `family`.

**Legacy:** Keep `GET /api/v1/recommendations/undercited` (SQL heuristic) only as fallback or short-term comparison if useful.

---

### 5. Recommended page as ranked product surface

**Goal:** Move from “demo baseline” to **decision support**.

**Deliverables**

- Update `apps/web/app/recommended/page.tsx` to consume the ranked endpoint.
- **Segment by family:** emerging / bridge / undercited.
- Show **explanation snippets** and **signal breakdowns** (aligned with `docs/build-brief.md` and existing tone on the page).

---

### 6. Trends from curated corpus topics

**Goal:** Show topic structure **inside the curated slice**, not generic OpenAlex-wide browsing.

**Deliverables**

- Aggregations over **included** (and optionally core) works only: `work_topics` + `topics` + `works`.
- Start simple: work count per topic, recent delta, optional “rising” label.
- `GET /api/v1/trends/topics` with query params as needed (`limit`, time window, etc.).
- Replace placeholder content on `apps/web/app/trends/page.tsx`.

**Placement:** High implementation value **early**; can proceed **in parallel** with ranking plumbing / v0 once topic list UX is stable—not after the entire ranking stack.

---

### 7. Evaluation and baseline comparison

**Goal:** Prove **judgment**, not just UI.

**Deliverables**

- **Baselines:** citation-sorted and date-sorted lists over the same candidate pool.
- **Comparison** surfaced via API and `apps/web/app/evaluation/page.tsx`.
- **Start instrumentation when ranking runs start:** persist enough metadata (e.g. `ranking_version`, snapshot id, run timestamps, config hash or notes) to **compare runs** cleanly—even before human labels exist.
- **Early metrics:** proxy metrics (diversity, recency mix, citation percentiles) are acceptable if **labeled as proxies** in the UI copy.
- **Later:** frozen benchmark labels, precision@k where labeled, freeze-at-T temporal backtest.

---

### 8. Embeddings and vector retrieval (2b / 3b)

**Goal:** Improve semantic relevance and bridge quality **without** blocking the first honest ranked release.

**Deliverables**

- Title+abstract embeddings → `embeddings` table; real `semantic_score` in `paper_scores`.
- Similar-paper retrieval (product or internal).
- Cluster assignment → stronger **bridge** scoring.

**Why not earlier:** Useful for quality, but not required to ship **versioned, explainable, ranked families** over the curated corpus.

---

## Dependency graph (short)

```
(1) Metadata contracts + list/detail UX
        ↓
(2) Ranking run + paper_scores plumbing
        ↓
(3) Ranking v0 write path (honest signal policy)
        ↓
(4) Ranked recommendations API
        ↓
(5) Recommended page

(6) Trends — parallel once (1) is stable; does not block (2)–(5)

(7) Evaluation instrumentation — starts with (2); UI expands as metrics mature

(8) Embeddings / retrieval — refines (3) and (4) signal semantics
```

---

## Positioning check

| OpenAlex-style browser | Research Radar |
|------------------------|----------------|
| Search, filter, detail, topic labels | **Curated** corpus policy as product asset |
| “What exists” | **Ranked families** + **what to notice now** |
| Opaque relevance | **Per-signal explanations** and honest versioning |
| Global or generic trends | **Corpus-scoped** trends |
| Popularity by default | **Measurable** comparison vs citation/date baselines (proxies first, labels later) |

---

## Guardrails

- No full OpenAlex ingest.
- No graph UI in V1.
- No LLM summary feature as a core dependency.
- No custom deep-model training (embeddings API usage is fine).
- No chat-with-papers detour.
- No broad expansion beyond the defined corpus slice without revisiting policy and evaluation.
