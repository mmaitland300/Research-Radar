# Roadmap

This document is the implementation sequence for V1: foundation -> ranking infrastructure -> ranked product -> trends and evaluation -> semantic refinement. It separates ranking plumbing (durable, honest) from signal quality (evolves over time).

## Where things stand

- Bootstrap / ingest: Corpus policy, OpenAlex bootstrap, Postgres schema, raw retention, and manifest counts are in place for the current slice.
- Live product slice: DB-backed search and paper detail; topic metadata flows through normalize + `work_topics`; list/detail IDs support full OpenAlex URLs where needed.
- Not yet product-complete: Corpus-scoped trends, evaluation vs baselines, embeddings-backed similarity UI, clustering / non-placeholder bridge and semantic scores in ranking.

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

**Goal:** Add representation learning and retrieval as a **separate seam** from ranking first: offline work-level vectors, then similar-paper read paths, then (later) optional use in `paper_scores`.

**Deliverables (sequenced — see ML milestone 1 below)**

- ML1a: Write path — title+abstract embeddings -> `embeddings` table, versioned by `embedding_version`, idempotent upserts.
- ML1b: Read path — nearest neighbors from stored vectors only (no live embed in API).
- ML1c: Product — similar papers on paper detail.
- ML1d: Quality — manual spot-check on anchor papers; notes for portfolio credibility.
- **Defer:** Forcing `semantic_score` into family ranking until there is a clear relevance target (seed, centroid, or query context). Cluster assignment remains a bridge / ML2 concern.

**Why not earlier:** Useful for quality, but not required to ship versioned, explainable, ranked families over the curated corpus. **Why retrieval before ranking integration:** Lower risk; uses existing `embeddings` + HNSW; avoids undefined “semantic relevance” for family feeds.

---

## ML milestones

These milestones make the project legible as a machine-learning portfolio piece instead of only a data product.

### ML milestone 1: embeddings + retrieval (paper-to-paper first)

**Strategy:** Ship **offline work embeddings** and **stored-vector nearest-neighbor retrieval** before touching ranking semantics. One vector per **included** work from **title + abstract** only (no chunking in v1). API reads never call the embed provider — only Postgres/pgvector.

**Schema already in place:** `embeddings(work_id, embedding_version, vector VECTOR(1536))` with PK `(work_id, embedding_version)`, `idx_embeddings_vector` (HNSW, cosine). `ranking_runs.embedding_version` is for **provenance later**, not required for ML1a.

**Visible outcome:** Similar papers for a known `paper_id`, demoable on paper detail, versioned by `embedding_version`.

**Primary dependency:** Step 1 stable list/detail/topic contracts; steps 2–5 can proceed in parallel. Do not block ML1 on ranking integration.

**Portfolio value:** Representation learning + versioned offline artifacts + retrieval design, explainable without conflating “family feed semantic score.”

**Out of scope for ML milestone 1 (defer):** Query-text semantic search, chunk embeddings, writing `semantic_score` into `paper_scores` without a defined relevance target.

---

#### ML1a — Embedding write path (implementation checklist)

**Goal:** Idempotently fill `embeddings` for included works missing a given `embedding_version`, using batched calls to the chosen provider (e.g. OpenAI-compatible embeddings → 1536 dims to match `VECTOR(1536)`).

**Deliverables**

- Select candidate works: `inclusion_status = 'included'`, optional corpus snapshot filter (same “latest snapshot with works” pattern as ranking), **anti-join** works with no row for `(work_id, embedding_version)`.
- Build **one text per work**: title + abstract (define empty-abstract behavior explicitly, e.g. title-only).
- Batch provider requests; handle rate limits / retries at orchestration layer.
- **UPSERT** into `embeddings` so re-runs are safe (same PK replaces vector + `created_at` behavior per your SQL policy).
- CLI entry point for operators.
- Tests: persistence SQL/upsert behavior (mocks or DB pattern used elsewhere); run orchestration with mocked provider.

**File-by-file checklist**

| Order | File | What to implement |
| ----- | ---- | ------------------- |
| 0 | `infra/db/schema.sql` | **No change** for ML1a — confirm table matches intent (1536-dim, PK, HNSW). Only edit here if a real migration is required. |
| 1 | `services/pipeline/pipeline/embedding_persistence.py` | **New.** DB-only helpers: resolve target `corpus_snapshot_version` (reuse or mirror `latest_corpus_snapshot_version_with_works` from ranking persistence), `list_work_ids_missing_embedding(conn, snapshot, embedding_version) -> list[WorkEmbeddingSource]` (or separate id + text query), `upsert_embeddings(conn, rows: Sequence[EmbeddingRow])`. Use `dict_row` consistently if returning dicts from `execute`. |
| 2 | `services/pipeline/pipeline/embedding_provider.py` (optional split) | **New (optional).** Thin wrapper: `embed_texts(texts: list[str]) -> list[list[float]]` with fixed model name + dimension assert (1536). Keeps `embedding_run.py` testable. Alternatively fold into `embedding_run.py` if you want fewer files. |
| 3 | `services/pipeline/pipeline/embedding_run.py` | **New.** `execute_embedding_run(...)` — open connection, list missing works, fetch title/abstract for those ids, chunk into batches, call provider, upsert in transactional batches, log counts / failures. No ranking or `paper_scores` writes. |
| 4 | `services/pipeline/pipeline/cli.py` | **Extend.** Subcommand e.g. `embed-works --embedding-version v1-title-abstract-1536` with `--corpus-snapshot-version`, `--database-url`, `--batch-size`, optional `--limit` for smoke. Wire to `execute_embedding_run`. |
| 5 | `services/pipeline/tests/test_embedding_persistence.py` | **New.** Unit-style tests for selection + upsert contracts (mock connection or test DB). |
| 6 | `services/pipeline/tests/test_embedding_run.py` | **New.** Mock provider + mock/patch DB; assert batching and upsert calls for a small fake corpus. |
| 7 | `pyproject.toml` / deps | Add HTTP client + any SDK only if needed; pin versions consistent with repo. |
| 8 | Env / ops docs | Document required secrets (e.g. API key), model id, and example CLI invocation in `docs/build-brief.md` or README slice — **only if** the repo already documents pipeline env elsewhere; avoid new markdown files unless the project already uses them for this. |

**ML1a exit criteria**

- Running `python -m pipeline.cli embed-works --embedding-version <label> ...` twice does not duplicate PK rows; second run processes only still-missing works.
- Spot check: `SELECT COUNT(*) FROM embeddings WHERE embedding_version = '<label>'` matches included works with text after full run.
- `services/pipeline/tests` green with new tests.

**ML1a explicit non-goals**

- No `GET /api/.../similar` (ML1b).
- No web UI (ML1c).
- No updates to `paper_scores.semantic_score` or ranking heuristics.

---

#### ML1b — Similar-papers API (read path)

**Goal:** `GET /api/v1/papers/{paper_id}/similar?limit=10&embedding_version=...` using **stored** vectors only; pgvector cosine distance; exclude self; return metadata + similarity + topics (reuse list/detail SQL patterns).

**File-by-file (outline for next seam)**

| File | Responsibility |
| ---- | ---------------- |
| `apps/api/app/similarity_repo.py` | **New.** Resolve internal `work_id` from OpenAlex id; fetch source vector; `<->` or `<=>` neighbor query with HNSW; map to response rows. |
| `apps/api/app/contracts.py` | **Extend.** Response models for similar items. |
| `apps/api/app/main.py` | **Extend.** New route under `/api/v1/papers/...`. |
| `apps/api/tests/test_similar_papers.py` | **New.** Monkeypatch repo; 404/503 cases. |

---

#### ML1c — Paper detail UI

**Goal:** “Similar papers” block on `apps/web/app/papers/[paperId]/page.tsx` using ML1b; reuse chips / result styles from search or recommended.

---

#### ML1d — Quality review (qualitative retrieval)

**Goal:** Lightweight judgment of neighbor quality **before** using retrieval in ranking. Portfolio-credible notes; honest failure modes.

**Immediate checks (after ML1c ships):**

- Confirm the **Similar papers** block in the browser with `NEXT_PUBLIC_EMBEDDING_VERSION` set.
- Spot-check **3–5** papers: neighbors should feel **semantically reasonable** for the corpus slice (e.g. MIR / music-data cohesion is a good first sign).

**Review pass (recommended shape):**

- Pick **5 anchor** papers (mix of topics, citation levels, one “bridgey” title if available).
- For each anchor, inspect **top 5** neighbors (API or UI).
- Mark each neighbor set **good**, **mixed**, or **weak**.
- Note recurring failure modes, for example:
  - same-venue bias
  - dataset-name / title-template bias
  - metadata noise
  - **abstract/title encoding issues** (mojibake, HTML entities — see cleanup task below)

**Output:** Short log (PR description, this doc, or private notes): anchor ids, verdict per set, and 1–2 bullets on systematic issues.

---

#### Cleanup task: normalized display and embedding text (non-embedding)

**Problem:** Some `works.title` / `works.abstract` strings show **encoding and entity artifacts** in UI and flow into **embedding input**, which hurts perceived quality and can skew neighbors (separate from retrieval math).

**Examples observed:** Mojibake such as `âEverything Corpusâ`; literal or double-encoded HTML entities (e.g. `\u0026amp;ndash;`).

**Direction:** Fix **upstream** in ingest/normalization (OpenAlex payload → stored text), optionally a shared sanitizer used at write time; avoid paper-by-paper patches in the API. Re-embed after a bulk fix if labels change materially.

**Priority:** Next cleanup after ML1d notes are captured — track explicitly so it is not confused with “bad embeddings.”

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

(8) Embeddings / retrieval -> ML1a–c similar-papers path first; optional later tie-in to (3)/(4) signals
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
- After step 8 / ML1a: Included works can be backfilled into `embeddings` for a version label without duplicating PK rows.
- After ML1b–c: Similar papers are available via API and visible on paper detail for a pinned `embedding_version`.
- After ML1d: Five-anchor / top-5-neighbor qualitative log exists (good / mixed / weak + failure modes); encoding cleanup task is tracked for ingest/display text.
- Later (ML2+): Semantic / bridge columns in `paper_scores` are filled from defined signals, not ad hoc.
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
