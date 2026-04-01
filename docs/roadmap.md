# Roadmap

This document is the implementation sequence for V1: foundation -> ranking infrastructure -> ranked product -> trends and evaluation -> semantic refinement. It separates ranking plumbing (durable, honest) from signal quality (evolves over time).

## Where things stand

- Bootstrap / ingest: Corpus policy, OpenAlex bootstrap, Postgres schema, raw retention, and manifest counts are in place for the current slice.
- **Implemented bootstrap sources:** `policy.py` ingests only the venues listed there — **TISMIR** and **JAES** today. `docs/build-brief.md` describes a broader long-term core-source allowlist (ISMIR, DAFx, ICASSP, etc.); those are not yet wired into `bootstrap-run` until added as `SourcePolicy` rows with OpenAlex source ids.
- Live product slice: DB-backed search and paper detail; topic metadata flows through normalize + `work_topics`; list/detail IDs support full OpenAlex URLs where needed.
- **Milestone 1 (low-cite pool):** The undercited recommendation family is gated on the frozen definition in `docs/candidate-pool-low-cite.md` (implemented in `in_low_cite_candidate_pool` / `build_step3_heuristic_score_rows`). Emerging and bridge still score every included work; semantic and bridge scores stay null; `reason_short` states the pool doc and that those signals are not modeled. CLI: `ranking-run --low-cite-min-year` / `--low-cite-max-citations` (defaults 2019 / 30).
- Not yet product-complete: Corpus-scoped trends, evaluation vs baselines, clustering / non-placeholder bridge and semantic scores in ranking. Similar-papers UI is gated on `NEXT_PUBLIC_EMBEDDING_VERSION` and stored `embeddings` rows.

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
- **Undercited scope:** Only papers in the low-cite candidate pool get an undercited row; citation popularity penalty for that family is normalized within pool members only. Changing thresholds requires updating the doc revision and `LOW_CITE_CANDIDATE_POOL_REVISION` in code when the definition changes, so ranking config and `reason_short` stay aligned with the written contract.

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

#### ML2 prototype execution scaffold (lean docs)

Use `docs/roadmap.md` for planning/tickets/gate criteria. Create `docs/ml2-bridge-review.md` only when there are real cluster/bridge outputs to review (not for planning prose).

**Hard constraints**

- Do not skip cluster inspection before bridge scoring.
- Pin provenance on every artifact (`corpus_snapshot_version`, `embedding_version`, `cluster_version`).

**Ticket order**

1. **ML2-1 Contract + versioning rules**
   - Define one assignment per `(work_id, cluster_version)` and deterministic `cluster_version` naming.
   - Lock write semantics so assignments are versioned and reproducible.
2. **ML2-2 Input slice (explicit provenance)**
   - Deterministic clustering inputs: included works, one `embedding_version`, one explicit `corpus_snapshot_version` (default latest only as convenience).
   - Persist resolved snapshot/version values on run metadata.
3. **ML2-3 Write path + CLI (idempotent)**
   - Add clustering CLI entrypoint and persistence helpers.
   - Re-run behavior must be explicitly defined and tested (overwrite-by-version or upsert with constraints).
4. **ML2-4 Minimal inspection API**
   - API-first inspection: cluster id, size, and sample paper titles.
   - Enough visibility to validate plausibility without direct DB poking.
   - Implemented: `GET /api/v1/clusters/{cluster_version}/inspect` (optional `sample_per_cluster`; response includes clustering metric note vs cosine similar-papers).
5. **ML2-6 Review worksheet scaffold starts here**
   - Start the qualitative worksheet structure once inspection exists; fill it during prototype runs.
   - Keep this lightweight and evidence-first.
6. **ML2-5a Bridge signal only (no ordering change)**
   - Persist `bridge_score` on bridge-family rows from cluster-boundary ratio (squared L2 vs centroids); `ranking-run --cluster-version` pins `clustering_runs` + snapshot + `embedding-version`.
   - Keep `FAMILY_WEIGHTS["bridge"].bridge` at 0 so `final_score` is unchanged; `semantic_score` stays null.
   - Missing cluster data: null `bridge_score` and explicit `reason_short`.
   - **DB validation (two `ranking_run_id` values: baseline without clustering artifact vs ML2-5a with it; same `corpus_snapshot_version` and `embedding_version`):** (1) sanity on `ranking_runs`; (2) bridge row counts must match; (3) invariance query must return **zero rows** (same `final_score` per `work_id` on bridge rows); (4) optional signal-delta query should show `bridge_score` and/or `reason_short` changed where ML2-5a applied.

```sql
-- (1) Sanity: same snapshot + embedding; clustering_artifact null vs set
SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, status,
       config_json->'clustering_artifact' AS clustering_artifact
FROM ranking_runs
WHERE ranking_run_id IN ('REPLACE_BASELINE_RANKING_RUN_ID', 'REPLACE_ML25A_RANKING_RUN_ID');

-- (2) Bridge row counts must match
SELECT ranking_run_id, COUNT(*) AS bridge_rows
FROM paper_scores
WHERE ranking_run_id IN ('REPLACE_BASELINE_RANKING_RUN_ID', 'REPLACE_ML25A_RANKING_RUN_ID')
  AND recommendation_family = 'bridge'
GROUP BY ranking_run_id
ORDER BY ranking_run_id;

-- (3) Strongest invariance: expect zero rows
WITH baseline AS (
    SELECT work_id, final_score, bridge_score, reason_short
    FROM paper_scores
    WHERE ranking_run_id = 'REPLACE_BASELINE_RANKING_RUN_ID'
      AND recommendation_family = 'bridge'
),
ml25a AS (
    SELECT work_id, final_score, bridge_score, reason_short
    FROM paper_scores
    WHERE ranking_run_id = 'REPLACE_ML25A_RANKING_RUN_ID'
      AND recommendation_family = 'bridge'
)
SELECT
    COALESCE(baseline.work_id, ml25a.work_id) AS work_id,
    baseline.final_score AS baseline_final_score,
    ml25a.final_score AS ml25a_final_score,
    baseline.bridge_score AS baseline_bridge_score,
    ml25a.bridge_score AS ml25a_bridge_score,
    LEFT(baseline.reason_short, 80) AS baseline_reason_prefix,
    LEFT(ml25a.reason_short, 80) AS ml25a_reason_prefix
FROM baseline
FULL OUTER JOIN ml25a USING (work_id)
WHERE baseline.work_id IS NULL
   OR ml25a.work_id IS NULL
   OR baseline.final_score IS DISTINCT FROM ml25a.final_score;

-- (4) Optional: expect some bridge_score or reason change when ML2-5a wired
SELECT
    COUNT(*) FILTER (
        WHERE baseline_bridge_score IS DISTINCT FROM ml25a_bridge_score
    ) AS rows_where_bridge_score_changed,
    COUNT(*) FILTER (
        WHERE baseline_reason IS DISTINCT FROM ml25a_reason
    ) AS rows_where_reason_changed,
    COUNT(*) AS bridge_rows_compared
FROM (
    SELECT
        b.work_id,
        b.bridge_score AS baseline_bridge_score,
        b.reason_short AS baseline_reason,
        m.bridge_score AS ml25a_bridge_score,
        m.reason_short AS ml25a_reason
    FROM paper_scores b
    JOIN paper_scores m
      ON b.work_id = m.work_id
     AND b.recommendation_family = 'bridge'
     AND m.recommendation_family = 'bridge'
    WHERE b.ranking_run_id = 'REPLACE_BASELINE_RANKING_RUN_ID'
      AND m.ranking_run_id = 'REPLACE_ML25A_RANKING_RUN_ID'
) s;
```

   - **Interpretation:** Step (3) any row => stop and debug. Step (3) empty and step (4) shows changes => ML2-5a behaved as intended (signal moved, `final_score` did not). Step (4) all zeros => cluster artifact likely did not affect bridge rows.
   - **Recorded check (2026-03-31, local `docker-compose` Postgres):** Same snapshot `source-snapshot-20260329-170012` and embedding `v1-title-abstract-1536-cleantext`. Baseline `rank-63710a0277` (`ml2-5a-val-baseline-20260331`, `clustering_artifact` null). ML2-5a `rank-c765e2de5c` (`ml2-5a-val-cluster-20260331`, cluster `kmeans-l2-v0-cleantext-k12`). Step (2): 38 bridge rows each. Step (3): **0** violation rows. Step (4): `bridge_score` differed on **10** / 38 joined rows; `reason_short` differed on **38** / 38. **Outcome:** matches the success rule (ordering invariant, signal moved); safe to treat as a defensible ML2-5a regression pass on this corpus before ML2-5b planning.
   - **Qualitative review before ML2-5b:** Use **k12** (or any artifact) for the SQL regression pass; use **k3** when you want a more interpretable cluster geometry for human list review. Pin the API with `ranking_run_id` (and snapshot / `ranking_version` when comparing): `GET /api/v1/recommendations/ranked?family=bridge&ranking_run_id=...`. Prefer `docs/ml2-bridge-review.md` only if you need worked examples and a longer pass/fail narrative.
   - **Recorded qualitative review (2026-03-31, local `docker-compose` Postgres):** Run **`rank-c34fa85261`** (`ml2-5a-qual-k3-20260331`) on `source-snapshot-20260329-170012`, embedding `v1-title-abstract-1536-cleantext`, cluster **`kmeans-l2-v0-cleantext-k3`**. **Checks:** **10 / 38** bridge rows return non-null `items[].signals.bridge`; those rows use the structural centroid-boundary `reason_short`; the rest use the explicit no-signal fallback, so copy does not overclaim semantics. **List shape:** With bridge weight still 0, bridge **top 10** overlaps emerging **top 10** on **8** papers (expected until weighting); overlap with undercited **top 10** is **4**. Papers that carry a numeric bridge score look like plausible cross-cutting MIR (datasets, regional/method breadth, multimodal corpora), not obvious random outliers.
   - **ML2-5b readiness (same review):** **Do not** treat this as automatic clearance for positive bridge weight. Most bridge rows still lack a numeric signal on this snapshot, so the family is not yet **meaningfully** separated from emerging at the head of the list, and weighting would barely move ordering until coverage improves. Treat ML2-5b as a **decision after** a denser or broader validation pass (more embedded works aligned with clustering, different k, or a larger corpus), reusing the same honesty and distinctness checks.
   - **ML2-5a qualitative API review (2026-04-01):** `GET /api/v1/recommendations/ranked?family=bridge&limit=20&ranking_version=ml2-5a-qual-k3-20260331&corpus_snapshot_version=source-snapshot-20260329-170012` returned run **`rank-c34fa85261`** on snapshot **`source-snapshot-20260329-170012`**, matching the intended pin and provenance.
   - **What the API showed:** Bridge-family rows behaved as designed for ML2-5a: some rows had non-null `signals.bridge` plus structural boundary wording, many rows still had `signals.bridge` null plus the honest fallback reason, and `final_score` remained the heuristic-only blend because bridge weight is still **0.0**.
   - **Verdict:** ML2-5a passes as an engineering milestone, but **ML2-5b is a no-go for now** because the bridge signal is still too sparse and the top of the list remains too emerging-shaped to justify weighting. **Mojibake** in several titles is recorded as a **separate corpus hygiene** issue, not a bridge-logic issue.
   - **What next for ML:**
     - Increase **embedding coverage** so more ranked bridge candidates can receive cluster-backed bridge scores. Use `python -m pipeline.cli embedding-coverage --embedding-version <label> [--corpus-snapshot-version ...] [--fail-on-gaps]` to audit gaps; run `embed-works` **without** `--limit` for full snapshot coverage (limit is for smoke tests only). `ranking-run --cluster-version` prints a **stderr warning** when any included work still lacks that embedding version. Re-run **`cluster-works`** after embeddings are complete so every embedded work gets an assignment.
     - Fix **mojibake / title-text normalization** at ingest (`clean_openalex_text` in `openalex_text.py`, used from `normalize.py`) and on existing rows via **`python -m pipeline.cli repair-works-text`** (`--dry-run` first). After changing stored title/abstract materially, prefer a **new `embedding_version`** (or delete and re-embed) so vectors match the cleaned text.
     - **Re-run clustering** and the same bridge review once the embedded slice is broader; revisit **ML2-5b** only if the bridge list becomes meaningfully more bridge-like and less sparse.

7. **ML2-5b Bridge affects ordering (when trusted)**
   - Small positive bridge weight for bridge family; recheck evaluation and lists.
8. **ML2-7 Evaluation compatibility only**
   - Ensure evaluation surfaces remain truthful/inspectable with non-placeholder bridge behavior.
   - No expanded benchmark claims in this step.
9. **ML2-8 Checklist-based go/no-go gate**
   - Written decision using explicit criteria: cluster quality, bridge distinctiveness, explanation honesty, evaluation sanity.

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

- No semantic signal in `paper_scores` until ML1d Pass 2 shows retrieval is good enough for the intended use case **and** the semantic relevance target for scoring is explicitly written down (seed, profile, query context, or similar).
- No full OpenAlex ingest.
- No graph UI in V1.
- No LLM summary feature as a core dependency.
- No custom deep-model training (embeddings API usage is fine).
- No chat-with-papers detour.
- No broad expansion beyond the defined corpus slice without revisiting policy and evaluation.
