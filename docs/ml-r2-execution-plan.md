# ML "r2" data execution plan

Operational sequence: **repair stored title/abstract text** -> **new embedding artifact** -> **full embed** -> **verify no gaps** -> **cluster** -> **verify assignments** -> **inspect via API** -> (optional) **new ranking run**.  
Implements the handoff described in `docs/roadmap.md` (clean text -> new `embedding_version` -> `cluster-works` -> inspect; bridge weight stays **0** until a separate ML2-5b decision).

---

## 1. Definitions (pin these before you start)

| Symbol | Meaning | Example |
|--------|---------|---------|
| `CORPUS_SNAPSHOT` | Corpus snapshot version string (must match rows in DB) | `source-snapshot-20260329-170012` |
| `EMBED_VER` | New label for `embeddings` rows; use a **new** label if `repair-works-text` changes any stored text | `v1-title-abstract-1536-cleantext-r2` |
| `CLUSTER_VER` | New label for `clusters` / `clustering_runs`; bump whenever geometry or inputs change | `kmeans-l2-v0-cleantext-r2-k3` |
| `API_BASE` | Running FastAPI base URL (no trailing slash) | `http://127.0.0.1:8000` |

**Labeling rules**

- If **dry-run** `rows_changed` is **0**: you may **reuse** the current `EMBED_VER` and only mint a new `CLUSTER_VER` if you still want a new k-means draw (document why).
- If **rows_changed** **> 0**: you **must** use a **new** `EMBED_VER` (or delete old vectors for that version - avoid in production); then use a **new** `CLUSTER_VER` tied to that embedding artifact.

---

**Existing databases (important)**

- `infra/db/schema.sql` only applies automatically when Postgres initializes a fresh volume.
- If your DB was created before the bridge-v2 columns existed, run this DDL manually before using the new path:

```sql
ALTER TABLE paper_scores ADD COLUMN IF NOT EXISTS bridge_eligible BOOLEAN NULL;
ALTER TABLE paper_scores ADD COLUMN IF NOT EXISTS bridge_signal_json JSONB NULL;
```

- Legacy rows stay `NULL` in these columns until you materialize new ranking runs with updated code.

---

## 2. Preconditions (must all pass before Phase A)

1. **Working directory**  
   `services/pipeline` (repo root is *not* required if `python -m pipeline.cli` resolves).

2. **Package install** (from repo root, once per venv)  
   `pip install -e ./services/pipeline`

3. **Database**  
   - Postgres reachable.  
   - `DATABASE_URL` set (PowerShell example):  
     `$env:DATABASE_URL = "postgresql://USER:PASS@HOST:5432/DBNAME"`  
   - Snapshot `CORPUS_SNAPSHOT` exists and has included works (same DB you use for the API).

4. **Embedding provider** (Phase C onward)  
   - `OPENAI_API_KEY` set (default model in CLI is `text-embedding-3-small`; override with `embed-works --model` if needed).

5. **API** (Phase F onward)  
   - API process running with the **same** `DATABASE_URL` (e.g. `uvicorn app.main:app --app-dir apps/api` from repo layout per `README.md`).

6. **Recorded values**  
   Open a log (ticket, PR, or scratch file) and copy **stdout/stderr** lines after each phase for `rows_changed`, `rows_written`, `missing_embedding`, `missing_cluster_assignment`, and printed IDs.

---

## 3. Phase A - Text repair (dry run, then commit)

**Goal:** Know how many included works would change; then apply cleanup if non-zero.

### A1. Dry run (required)

```powershell
cd "path\to\Research-Radar\services\pipeline"
$env:DATABASE_URL = "postgresql://..."   # your URL

python -m pipeline.cli repair-works-text `
  --corpus-snapshot-version CORPUS_SNAPSHOT `
  --dry-run
```

**Read stderr** for:

`repair-works-text (dry-run): corpus_snapshot_version=... scanned=... rows_changed=N`

**Record `N`.**

### A2. Apply repair (only if `N > 0`)

```powershell
python -m pipeline.cli repair-works-text `
  --corpus-snapshot-version CORPUS_SNAPSHOT
```

**Read stderr** for `rows_changed=` again; it should match expectations.

### A3. Decision

- **`N = 0`** -> Skip A2. Proceed to Phase B only if you still need a **new** `EMBED_VER` / `CLUSTER_VER` for other reasons; otherwise you may skip embed/cluster or use a **new** `CLUSTER_VER` only (document).
- **`N > 0`** -> You **must** use a **new** `EMBED_VER` in Phase C (do not reuse the pre-repair embedding label for "truth" vectors).

---

## 4. Phase B - Choose artifact labels

1. Set `EMBED_VER` (new if Phase A changed rows or you changed text earlier).  
2. Set `CLUSTER_VER` (always new for a new k-means identity; include snapshot + embed + k in the string for traceability).  
3. Write them in your log; all later commands must use the **same** strings.

---

## 5. Phase C - Full embedding backfill

**Goal:** Every included work in the snapshot has a row in `embeddings` for `EMBED_VER`.

```powershell
python -m pipeline.cli embed-works `
  --embedding-version EMBED_VER `
  --corpus-snapshot-version CORPUS_SNAPSHOT
```

**Do not** pass `--limit` for production coverage (it is for smoke tests only).

**Verify stderr summary:** `still_missing_after_run=0` after a successful full run. If not, fix errors (rate limits, API key, DB) and re-run; `embed-works` is idempotent on PK `(work_id, embedding_version)`.

---

## 6. Phase D - Embedding coverage gate

**Goal:** Fail the script if any included work lacks an embedding.

```powershell
python -m pipeline.cli embedding-coverage `
  --embedding-version EMBED_VER `
  --corpus-snapshot-version CORPUS_SNAPSHOT `
  --fail-on-gaps
```

**Success:** Process exits **0**. Stderr includes `missing_embedding=0`.

**Failure:** Exit code **1** if `missing_embedding > 0`. Do not proceed to clustering until this passes.

**Note:** Do **not** pass `--cluster-version` here yet (cluster run does not exist).

---

## 7. Phase E - Clustering run

**Goal:** Write `clustering_runs` + per-work cluster assignments for `(CORPUS_SNAPSHOT, EMBED_VER)`.

```powershell
python -m pipeline.cli cluster-works `
  --embedding-version EMBED_VER `
  --cluster-version CLUSTER_VER `
  --corpus-snapshot-version CORPUS_SNAPSHOT `
  --cluster-count 3 `
  --max-iterations 20
```

Adjust `--cluster-count` / `--max-iterations` per your review plan; defaults exist in `cli.py` if you omit them.

**Verify stderr:** `status=succeeded`, `clustered_works` equals expected input size for embedded works in that snapshot.

---

## 8. Phase D2 - Cluster assignment coverage (recommended)

**Goal:** Fail if any included work is missing a cluster row for `CLUSTER_VER`.

```powershell
python -m pipeline.cli embedding-coverage `
  --embedding-version EMBED_VER `
  --corpus-snapshot-version CORPUS_SNAPSHOT `
  --cluster-version CLUSTER_VER `
  --fail-on-gaps
```

**Success:** Exit **0**, stderr includes `missing_cluster_assignment=0`.

**Failure:** Exit **1** if assignments missing; exit **2** if no `clustering_runs` row matches `(CLUSTER_VER, CORPUS_SNAPSHOT)` - that means Phase E did not complete or labels typo.

---

## 9. Phase F - API inspection (human + machine)

**Goal:** Plausible cluster titles/samples before any bridge-weight or ML2-5b decision.

1. Confirm API is up: `GET API_BASE/readyz` -> 200.
2. Inspect clusters:

```powershell
Invoke-RestMethod "$API_BASE/api/v1/clusters/CLUSTER_VER/inspect?sample_per_cluster=5" | ConvertTo-Json -Depth 8
```

(Bash: `curl -s "$API_BASE/api/v1/clusters/$CLUSTER_VER/inspect?sample_per_cluster=5"`)

**Record** qualitative notes (cluster coherence, junk cluster, obvious mis-assignments) in your log or `docs/roadmap.md` / review doc per team practice.

---

## 10. Phase G - Optional: new ranking run (bridge signal still non-ordering)

**Goal:** Materialize `paper_scores` with ML2-5a-style `bridge_score` on rows where clustering applies; **`final_score` unchanged** while bridge weight remains **0** in app config.

Pick a **new** `RANK_VER` string (e.g. `ml2-5a-cleantext-r2-k3-YYYYMMDD`).

```powershell
python -m pipeline.cli ranking-run `
  --ranking-version RANK_VER `
  --corpus-snapshot-version CORPUS_SNAPSHOT `
  --embedding-version EMBED_VER `
  --cluster-version CLUSTER_VER `
  --bridge-weight-for-family-bridge 0
```

For an **ML2-5b** experiment (bridge signal in `final_score` for the bridge family only), set **`--bridge-weight-for-family-bridge`** to a small positive **W** in **[0.0, 0.25]** (same snapshot/cluster/embed as your ML2-5a control for a fair diff). Omit the flag or use **0** for ML2-5a-style runs. Example (pin IDs to your environment): `python -m pipeline.cli ranking-run --ranking-version ml2-5b-exp-r2-k3-w006-YYYYMMDD --embedding-version v1-title-abstract-1536-cleantext-r2 --cluster-version kmeans-l2-v0-cleantext-r2-k3 --corpus-snapshot-version source-snapshot-20260329-170012 --bridge-weight-for-family-bridge 0.06`

**Capture stdout:** first line is `ranking_run_id` (pin this in API queries and evaluations).

**Guardrails:** Do not ship production weighting until roadmap ML2-5b criteria are met; do not set `semantic_score` via this path.

---

## 11. Definition of done (minimum for "r2 path" complete)

- [ ] Phase A: dry-run recorded; apply step run iff `rows_changed > 0`.  
- [ ] `EMBED_VER` / `CLUSTER_VER` documented.  
- [ ] Phase C + D: `missing_embedding=0` with `--fail-on-gaps`.  
- [ ] Phase E: clustering `succeeded`.  
- [ ] Phase D2: `missing_cluster_assignment=0` with `--fail-on-gaps`.  
- [ ] Phase F: inspect JSON reviewed and notes filed.  
- [ ] (Optional) Phase G: new `ranking_run_id` recorded for downstream UI/API pins.

---

## 12. Failure triage (short)

| Symptom | Likely cause |
|---------|----------------|
| `embed-works` quota / auth errors | `OPENAI_API_KEY`, billing, or `--model` mismatch |
| `still_missing_after_run > 0` | Partial run; fix errors and re-run without `--limit` |
| `embedding-coverage` exit 1 | Typo in `EMBED_VER` or snapshot; or not all works embedded |
| `embedding-coverage` exit 2 with `--cluster-version` | Clustering not written for that `CLUSTER_VER` + snapshot |
| Empty / nonsense clusters | Wrong `cluster_count`, bad text still in DB, or embedding/model mismatch - revisit Phase A/C |

---

## 13. Reference - command index

All subcommands: `python -m pipeline.cli --help`  
Per command: `python -m pipeline.cli <command> --help`

Implemented in `services/pipeline/pipeline/cli.py`: `repair-works-text`, `embed-works`, `embedding-coverage`, `cluster-works`, `ranking-run`.
