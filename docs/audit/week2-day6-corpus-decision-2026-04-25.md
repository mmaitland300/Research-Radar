# Week 2 Day 6 - Corpus decision gate

Date: `2026-04-25`  
Owner: `@mmaitland300`  
Status: `complete`

## Decision summary

- **Go:** use current stack for smoke/demo labeling and evaluator workflow validation.
- **No-go:** do not treat this corpus as sufficient for strong ML-quality benchmark claims.
- **Next:** expand corpus, cut a new snapshot, and rerun embedding + clustering + ranking before serious benchmark labeling.

## 1) Current corpus state

- `corpus_snapshot_version`: `source-snapshot-20260425-044015`
- Included works: `59`
- Excluded works: `517`
- Source scope currently implemented in bootstrap policy: TISMIR + JAES.
- Known limitation: small, venue-constrained slice (TISMIR/JAES-heavy), suitable for plumbing validation and UX sanity checks, not broad external quality claims.

## 2) Labels allowed on 59 works

- Smoke labels for UI/evaluation flow checks.
- Rubric testing and adjudication workflow calibration.
- Demo sanity checks for family behavior (`emerging` / `bridge` / `undercited`).
- Spot-checking explanation and provenance wiring.

## 3) Labels and claims deferred until corpus expansion

- Precision@k or benchmark-strength retrieval/recommendation claims.
- Semantic reranking quality claims.
- Bridge-quality claims with broad generalization.
- Trained-model readiness claims.

## 4) Rationale

- The current corpus is strong enough to validate pipeline wiring, ranking reproducibility, and evaluation mechanics.
- The corpus is too narrow/small to support robust ML-quality or portfolio-strength performance claims.
- Expanding sources first reduces risk of overfitting conclusions to a constrained slice.

## 5) Decision

- **Decision:** Proceed with smoke/demo evaluation only on the current stack.
- **Gate:** No strong ML benchmark claims until corpus expansion and rerun artifacts are complete.
- **Execution path:** Prefer "expand corpus first" before Day 7/8 benchmark-style labeling.
