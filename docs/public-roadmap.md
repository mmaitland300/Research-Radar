# Public Roadmap

Last updated: 2026-06-15

Research Radar is a working prototype for explainable MIR/audio-ML paper
ranking. This roadmap tracks the next public improvements that make the project
easier to evaluate without overstating the recommender quality. Detailed
planning remains in [internal/roadmap.md](internal/roadmap.md); this file is the
public-facing version.

## Current Baseline

- Live app: search, recommended feeds, paper detail, trends, and evaluation.
- Local no-key demo: fixture-mode API and web app through `npm run demo:local`.
- Full local path: Postgres, pgvector, OpenAlex ingest, pipeline, API, and web.
- Evidence: CI, fixture/API tests, evaluation docs, slim runtime audit
  artifacts, and release notes.
- Current curated-surface marker: `v0.2.0`
  ([release notes](releases/v0.2.0.md)).
- Emerging may be ordered by the bounded ML scorer when the live gate is
  configured; evaluation is still not a validation claim.

## Near-Term Work

### 1. Release And Repository Hardening

Goal: make the public repo safer and easier to review.

- Keep CI required before merging to `main`.
- Done: dependency-update automation (Dependabot) and CodeQL code scanning.
- Done: public baseline release `v0.1.0-public-baseline` with notes,
  smoke-check evidence, and known limitations
  ([release notes](releases/v0.1.0-public-baseline.md)).
- Done: audit boundary cleanup moved historical governance/process artifacts to
  `archive/ml-governance-audit`; `docs/audit/` now keeps only runtime-required
  frozen artifacts.
- Done: pipeline CLI split into family-scoped modules while preserving command
  names and arguments.
- Done: FastAPI route handlers split into domain routers while preserving the
  route method/path surface.
- Done: `v0.2.0` release marker for the curated public surface
  ([release notes](releases/v0.2.0.md)).
- Done: `EVALUATION.md` slimmed into a shorter conclusions-only status guide.
- Done: `apps/web/app/recommended/page.tsx` split into route-local
  components and modules.
- Done: frontend dependency alignment and ESLint flat-config cleanup.
- Done: Python 3.12 constraints-lock workflow for CI/dev installs.
- Future: add lightweight frontend tests.

### 2. Evaluation Credibility

Goal: improve evidence quality beyond proxy and single-reviewer checks.

- Future: freeze a small labeled review set for recommendation families.
- Future: report precision-at-k or a similarly simple labeled metric where coverage is
  sufficient.
- Keep proxy metrics clearly labeled as iteration signals, not validation.

### 3. Corpus Expansion

Goal: broaden the useful research surface while preserving provenance.

- Future: add the next source policy rows only after their OpenAlex source ids and
  inclusion boundaries are documented.
- Preserve snapshot identifiers and ingest manifests for repeatability.
- Keep TISMIR + JAES as the narrow baseline while expansion work is reviewed.

### 4. Product Review Path

Goal: let a reviewer understand the system quickly.

- Keep the no-key demo deterministic.
- Maintain one short review path from README to live app, evaluation boundary,
  release notes, and roadmap issues.
- Prefer small UI improvements that expose provenance, ranking version, and
  known limits over broad visual redesigns.

### 5. ML Scoring Rollout

Goal: advance learned scoring only through explicit gates.

- Keep shadow or audit modes isolated until gate checks pass.
- Preserve run ids, scorer versions, and audit bundle links.
- Do not describe learned scoring as production validated until the evaluation
  evidence supports that claim.

## What Is Not Claimed Yet

- Research Radar is not a validated recommender system.
- The corpus is intentionally narrow until expansion is documented and reviewed.
- Bridge diagnostics remain experimental.
- Proxy metrics are useful for iteration, but they are not a substitute for
  reviewer-labeled usefulness evaluation.
