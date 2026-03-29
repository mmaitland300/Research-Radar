# Roadmap

This document is the implementation sequence for V1: foundation -> ranking infrastructure -> ranked product -> trends and evaluation -> semantic refinement. It separates ranking plumbing (durable, honest) from signal quality (evolves over time).

## Where things stand

- Bootstrap / ingest: Corpus policy, OpenAlex bootstrap, Postgres schema, raw retention, and manifest counts are in place for the current slice.
- Live product slice: DB-backed search and paper detail; topic metadata flows through normalize + `work_topics`; list/detail IDs support full OpenAlex URLs where needed.
- Not yet product-complete: Materialized `paper_scores` writes, ranked recommendations API, corpus-scoped trends, evaluation vs baselines, embeddings-backed semantic and bridge signals.

---

## Portfolio framing

This project is intended to produce three portfolio assets, not just one:

1. Data and systems asset
   - reproducible corpus policy
   - snapshot/versioned ingest
   - raw payload retention
   - debuggable manifests and watermarks

2. ML asset
   - embeddings for title + abstract
   - similarity retrieval
   - clustering / bridge detection
   - ranking signals and scored recommendation families
   - evaluation against citation/date baselines and temporal backtests

3. Product asset
   - search
   - recommended feeds
   - paper detail
   - trends
   - explainability

Current status: the repo is strong on the data/systems asset, has an initial product slice, and has not yet surfaced the most portfolio-visible ML milestones.

---

## Recommended build sequence

### 1. Metadata contracts + UX foundation

**Goal:** Make ingest visible in the UI and stabilize paper/topic contracts before ranking and clients depend on them.

**Deliverables**

- Extend the list API so each item includes top 2-3 topic names per paper, using one SQL shape (aggregation / join / subquery), with no N+1 per row.
- Search: topic chips on `apps/web/app/search/page.tsx`.
- Paper detail: topic chips, clearer source block, optional source display name on the detail contract if missing from the API/repo query.
- Skip `GET /api/v1/topics` until autocomplete work actually starts.

**Why first:** Proves topic data in-product; hardens list/detail contracts (including identifier and field stability) before ranking layers consume them.

---

### 2. Ranking data model and run plumbing

**Goal:** Durable infrastructure so Research Radar is a ranking product, not a browser, without pretending stub signals are final science.

**Deliverables**

- Write path for `ranking_runs`: insert rows keyed by `ranking_run_id`, with `ranking_version` as the algorithm/config label plus `corpus_snapshot_version`, `embedding_version`, status, timestamps, config, and notes.
- Write path for `paper_scores`: persist `ranking_run_id`, `recommendation_family`, `final_score`, per-signal columns, and foreign keys so every score row ties to corpus state via `ranking_runs`.
- Contract clarity: document which fields are real v0 vs placeholder / not yet modeled. Do not present silent zeros as authoritative semantic or bridge scores.
- Persist enough run metadata from the first score write to compare runs later.

**Why before fancy scoring:** Tables and runs are the backbone; heuristics can evolve without reshaping the product architecture.

---

### 3. Ranking v0 (honest heuristic pass)

**Goal:** First materialized ranked outputs for all three families: emerging, bridge, undercited.

**Signal policy**

- Use real proxies where possible: citation velocity, recency, topic prevalence, or simple growth proxies from curated corpus + `work_topics`.
- Be explicit where not modeled: semantic relevance and bridge score may be null, omitted in API, or labeled `not_yet_modeled` until embeddings/clusters exist. Prefer honesty over fake richness.
- Version naming: e.g. `v0-heuristic-no-embeddings` so consumers know what they are getting.
- Keep `final_score` aligned with the weighting strategy in `docs/build-brief.md` and API settings, even if some signals are not yet modeled.

---

### 4. Ranked recommendations API + explanation contract

**Goal:** Expose the core differentiator: what to pay attention to now, and why.

**Deliverables**

- New API module for the read path (`paper_scores` -> `works`, optional topics).
- Single endpoint style: `GET /api/v1/recommendations/ranked?family=emerging|bridge|undercited&limit=...` with optional `ranking_version`; default = latest successful `ranking_run_id` for the current snapshot.
- Response fields: `paper_id`, `title`, `year`, `citation_count`, `source_slug`, `topics`, per-signal map, `final_score`, `reason_short`, `ranking_version`, `ranking_run_id`, `family`.

**Legacy:** Keep `GET /api/v1/recommendations/undercited` (SQL heuristic) only as fallback or short-term comparison if useful.

---

### 5. Recommended page as ranked product surface

**Goal:** Move from demo baseline to decision support.

**Deliverables**

- Update `apps/web/app/recommended/page.tsx` to consume the ranked endpoint.
- Segment by family: emerging / bridge / undercited.
- Show explanation snippets and signal breakdowns.
- Surface `ranking_version` somewhere in the UI or page metadata so the feed is inspectable.

---

### 6. Trends from curated corpus topics

**Goal:** Show topic structure inside the curated slice, not generic OpenAlex-wide browsing.

**Deliverables**

- Aggregations over included (and optionally core) works only: `work_topics` + `topics` + `works`.
- Start simple: work count per topic, recent delta, optional rising label.
- `GET /api/v1/trends/topics` with query params as needed (`limit`, time window, etc.).
- Replace placeholder content on `apps/web/app/trends/page.tsx`.

**Placement:** High implementation value early; can proceed in parallel with ranking plumbing / v0 once topic list UX is stable.

---

### 7. Evaluation and baseline comparison

**Goal:** Prove judgment, not just UI.

**Deliverables**

- Baselines: citation-sorted and date-sorted lists over the same candidate pool.
- Comparison surfaced via API and `apps/web/app/evaluation/page.tsx`.
- Start instrumentation when ranking runs start: persist enough metadata (`ranking_version`, snapshot id, run timestamps, config hash or notes) to compare runs cleanly even before human labels exist.
- Early metrics: proxy metrics (diversity, recency mix, citation percentiles) are acceptable if labeled as proxies in the UI copy.
- Later: frozen benchmark labels, precision@k where labeled, freeze-at-T temporal backtest.

**Important:** Proxy metrics help with iteration but are not substitutes for labeled relevance evaluation.

---

### 8. Embeddings and vector retrieval (2b / 3b)

**Goal:** Improve semantic relevance and bridge quality without blocking the first honest ranked release.

**Deliverables**

- Title+abstract embeddings -> `embeddings` table; real `semantic_score` in `paper_scores`.
- Similar-paper retrieval (product or internal).
- Cluster assignment -> stronger bridge scoring.

**Why not earlier:** Useful for quality, but not required to ship versioned, explainable, ranked families over the curated corpus.

---

## ML milestones

These milestones make the project legible as a machine-learning portfolio piece instead of only a data product.

### ML milestone 1: embeddings + retrieval

**Visible outcome:** Similarity-based retrieval over the curated corpus, powered by title + abstract embeddings.

**Primary steps:** Builds primarily on step 8, after steps 1-5 establish stable paper contracts and product surfaces.

**What ships**

- embedding generation job with `embedding_version`
- persisted vectors in `embeddings`
- nearest-neighbor retrieval for a paper or query
- an internal or user-facing similar-papers surface

**Portfolio value:** Demonstrates representation learning usage, retrieval design, and versioned offline ML artifacts.

### ML milestone 2: clustering + bridge score

**Visible outcome:** Papers can be identified as connecting neighboring clusters rather than only matching keywords or citation counts.

**Primary steps:** Builds on steps 2-5 and step 8, using ranking plumbing plus learned structure.

**What ships**

- cluster assignment versioning
- bridge-oriented features using embedding neighborhoods and/or citation context
- non-placeholder `bridge_score` in `paper_scores`

**Portfolio value:** Demonstrates unsupervised structure discovery and a domain-specific recommendation signal.

### ML milestone 3: evaluated ranking

**Visible outcome:** Ranked families outperform simple citation/date sorting on explicit or proxy metrics.

**Primary steps:** Centers on step 7, and depends on steps 2-5 for versioned runs, ranked outputs, and recommendation surfaces.

**What ships**

- versioned ranking runs
- compared baselines
- evaluation page with methodology notes
- temporal backtest when labels are ready

**Portfolio value:** Demonstrates applied ranking, experimentation discipline, and honest model evaluation.

---
## Dependency graph (short)

```text
(1) Metadata contracts + list/detail UX
        ->
(2) Ranking run + paper_scores plumbing
        ->
(3) Ranking v0 write path (honest signal policy)
        ->
(4) Ranked recommendations API
        ->
(5) Recommended page

(6) Trends -> parallel once (1) is stable; does not block (2)-(5)

(7) Evaluation instrumentation -> starts with (2); UI expands as metrics mature

(8) Embeddings / retrieval -> refines (3) and (4) signal semantics
```

---

## Milestone exit criteria

- After step 1: List API returns `topics: list[str]` with at most 3 topic names per paper, ordered by `work_topics.score DESC` then `topics.name ASC`. Search page renders those topics as chips. Detail page renders topic chips from Postgres-backed detail data and keeps or improves the source block (venue as primary label). List API uses one SQL query for papers plus top topics (no N+1). `source_label` is present on list items when venue join resolves.
- After step 2: Schema uses `ranking_run_id` as run PK; `paper_scores` references it with nullable signal columns and required `reason_short`. `ranking-run` CLI persists `running` then `succeeded` or `failed` with `config_json` / `counts_json`. Multiple runs may share the same `ranking_version`.
- After step 3: `paper_scores` contains heuristic-ranked rows for all three recommendation families (beyond Step-2 stubs).
- After step 4: Ranked recommendations can be fetched by family for the latest successful run.
- After step 5: `/recommended` is powered by ranked data rather than the heuristic-only baseline.
- After step 6: `/trends` is powered by curated-corpus topic aggregations rather than placeholder copy.
- After step 7: Evaluation page compares ranked output to citation/date baselines and clearly labels proxy metrics.
- After step 8: Semantic relevance and bridge signals are no longer placeholder-only.
- After ML milestone 1: The repo can demo embedding-backed similarity retrieval over live corpus papers.
- After ML milestone 2: `bridge_score` is computed from learned or clustered structure rather than a placeholder.
- After ML milestone 3: The ranking story includes measured comparison against simple baselines.

---

## Positioning check

| OpenAlex-style browser | Research Radar |
|------------------------|----------------|
| Search, filter, detail, topic labels | Curated corpus policy as product asset |
| "What exists" | Ranked families + what to notice now |
| Opaque relevance | Per-signal explanations and honest versioning |
| Global or generic trends | Corpus-scoped trends |
| Popularity by default | Measurable comparison vs citation/date baselines (proxies first, labels later) |

---

## Guardrails

- No full OpenAlex ingest.
- No graph UI in V1.
- No LLM summary feature as a core dependency.
- No custom deep-model training (embeddings API usage is fine).
- No chat-with-papers detour.
- No broad expansion beyond the defined corpus slice without revisiting policy and evaluation.
