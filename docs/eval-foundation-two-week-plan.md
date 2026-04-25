# Two-Week Execution Plan (Evaluation-First)

Purpose: move from a working prototype to an ML project with measurable outcomes, reproducible artifacts, and a safe path to bridge/semantic/trained-model experiments.

## Current Known-Good Stack

- `corpus_snapshot_version`: `source-snapshot-20260425-044015`
- `embedding_version`: `v1-title-abstract-1536-cleantext-r3`
- `cluster_version`: `kmeans-l2-v0-cleantext-r3-k6`
- `ranking_version`: `bridge-v2-nm1-zero-r3-k6-20260424`
- `ranking_run_id`: `rank-3904fec89d`
- included works: `59`

---

## Week 1 (Stabilize + Freeze)

### Day 1 - Baseline freeze and provenance record

Deliverables:
- A single baseline record with:
  - snapshot / embedding / cluster / ranking version
  - ranking run id
  - included/excluded counts
- Link or path to screenshots for:
  - `/recommended?family=emerging`
  - `/recommended?family=bridge`
  - `/recommended?family=undercited`
  - `/evaluation?family=emerging`
  - `/evaluation?family=bridge`
  - `/evaluation?family=undercited`

Done criteria:
- Anyone can reconstruct what "baseline" means without asking in chat.

### Day 2 - Production pin + deploy verification

**Status:** `complete` (2026-04-25). Evidence: `docs/audit/week1-day2-production-pins-2026-04-25.md` and screenshot `docs/audit/screenshots/2026-04-25-baseline/railway-project-next-public-pins.png`.

Deliverables:
- Confirm production pins are set and aligned with baseline:
  - `NEXT_PUBLIC_EMBEDDING_VERSION`
  - `NEXT_PUBLIC_RANKING_VERSION`
- Verify Railway has a succeeded run matching the pinned `ranking_version`.
- Record evidence (CLI output or screenshots).

Done criteria:
- Browser behavior matches pinned versions, not implicit latest defaults.

### Day 3 - Copy consistency pass

**Status:** `complete` (2026-04-25). Evidence: `docs/audit/week1-day3-copy-consistency-2026-04-25.md`.

Deliverables:
- Ensure wording alignment across:
  - trends page
  - recommended page
  - recommendation-family API descriptions
  - product/meta endpoint language that summarizes ranking behavior
  - undercited API description
  - candidate pool definition doc

Done criteria:
- No contradictory copy about scope, candidate pools, ranking semantics, or signal usage claims.

### Day 4 - Regression guardrails

**Status:** `complete` (2026-04-25). Evidence: focused test set passed -
`apps/api/tests/test_recommendations_ranked.py`, `apps/api/tests/test_evaluation_compare.py`,
`apps/api/tests/test_scores_repo.py`, `services/pipeline/tests/test_ranking_run.py`.

Deliverables:
- Tests for:
  - evaluation compare same-pool behavior
  - ranking version resolution logic
  - zero bridge-weight invariance behavior

Done criteria:
- Expected regressions fail tests before they reach main.

### Day 5 - Ops runbook

**Status:** `complete` (2026-04-25). Evidence: `docs/audit/week1-day5-ops-runbook-2026-04-25.md`.

Deliverables:
- Short runbook for:
  - ingest stuck/running repair
  - embedding coverage checks
  - cluster coverage checks
  - ranking run status checks

Done criteria:
- A teammate can recover pipeline state from docs only.

---

## Week 2 (ML Evaluation Foundation)

### Day 6 - Corpus decision gate

**Status:** `complete` (2026-04-25). Evidence: `docs/audit/week2-day6-corpus-decision-2026-04-25.md`.

Question:
- Is `59` included works sufficient for the next evaluation pass?
- If not, expand sources first and cut a new snapshot.

Current recommendation: `59` included works is acceptable for smoke/demo labels, but expand corpus before making strong ML-quality benchmark claims.

Deliverables:
- Written go/no-go decision with rationale.

Done criteria:
- No ambiguity on whether labels are being collected on current or expanded corpus.

### Day 7 - Retrieval benchmark set

Deliverables:
- 20-50 canonical search/retrieval prompts.
- For each prompt: expected-good paper ids (or notes on acceptable alternatives).

Done criteria:
- A stable benchmark file exists and can be rerun against new versions.

### Day 8 - Recommendation judgment set

Deliverables:
- Human labels for top results in each family (`emerging`, `bridge`, `undercited`):
  - `good`
  - `acceptable`
  - `miss`
  - `irrelevant`
- A one-paragraph rubric pinned in the same doc.

Done criteria:
- At least one complete labeled pass exists for the baseline run stack.

### Day 9 - Experiment matrix (after labels exist)

Deliverables:
- Experiment table with explicit hypotheses:
  - baseline
  - zero-weight bridge (signal persisted, no blend)
  - small bridge-weight variants
  - optional semantic-assist variant
- Planned comparison metric against Day 8 labels.

Done criteria:
- Every experiment has a measurable success/failure criterion.

### Day 10 - Decision bundle

Deliverables:
- Package:
  - run stack
  - screenshots
  - retrieval benchmark
  - recommendation judgments
  - experiment matrix
  - "what we can train/evaluate next"

Done criteria:
- Clear readiness statement:
  - ready for ML2 prototype iteration?
  - ready/not ready for product integration?
  - ready/not ready for custom-model training path?

---

## Working Rules

- Do not blend semantic signals into product ranking without a defined relevance target and judged improvements.
- Record provenance for every artifact:
  - `corpus_snapshot_version`
  - `embedding_version`
  - `cluster_version`
  - `ranking_version`
  - `ranking_run_id`
- Prefer "evidence changed" over "numbers changed".
