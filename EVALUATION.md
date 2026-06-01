# Evaluation Status

This page is the short status guide for Research Radar. It explains what has
been checked, what is still proxy-only, and what the project does not yet
support as a model-quality claim.

## Short Version

- Research Radar has working search, recommendations, paper detail, trends, and
  evaluation surfaces backed by versioned ranking runs.
- The main public evaluation page compares ranked output against citation/date
  baselines. Those are useful proxy checks, not human relevance validation.
- A complete single-reviewer top-20 labeling pass exists for one pinned baseline
  run across `emerging`, `bridge`, and `undercited`.
- Bridge diagnostics are useful for analysis, but bridge is still experimental
  and not default-ready.
- The current checked-in corpus slice is small and narrow enough that strong ML
  benchmark claims are deferred until corpus expansion and a larger review
  protocol.

## Current Live Run vs Archived Baseline

The public app can show the current deployed run, while this document records
the stable baseline used for repeatable review.

| Reference | What it means | How to use it |
| --- | --- | --- |
| Live app | Current deployed UI and API behavior, including whatever run the deployment config resolves | Good for interactive review; read the visible run metadata before comparing results |
| Pinned baseline | Frozen `2026-04-25` run stack recorded below and in the audit folder | Use for documented claims, screenshots, and stable comparisons |
| URL-pinned run | A specific `ranking_run_id` supplied in the URL or API request | Use when checking one materialized run without relying on latest-run resolution |
| Fixture mode | Checked-in toy data behind `npm run demo:local` | Use for setup and route-shape checks only; do not cite as ranking validation |

If the live app and the archived baseline differ, treat that as expected unless
the deployment is supposed to be pinned to the same run. Record the visible
`ranking_run_id`, `ranking_version`, `corpus_snapshot_version`, and
`embedding_version` before drawing conclusions.

## Current Pinned Baseline

The original baseline freeze is dated `2026-04-25`. A second surface was added in May 2026 for shadow evaluation, and the live Emerging feed now uses a bounded ML scorer on that surface when the deployment gate is enabled.

| Field | Baseline (2026-04-25) | Live Emerging (2026-05-31) |
| --- | --- | --- |
| `corpus_snapshot_version` | `source-snapshot-20260425-044015` | `source-snapshot-shadow-generalization-v1-20260521` |
| `embedding_version` | `v1-title-abstract-1536-cleantext-r3` | `shadow-generalization-text-embedding-v1` |
| `ranking_version` | `bridge-v2-nm1-zero-r3-k6-20260424` | `shadow-generalization-product-candidate-ranking-v1` |
| `ranking_run_id` | `rank-3904fec89d` | `rank-83787b91ef` |
| Included works | `59` | `528` |
| Source scope | TISMIR + JAES | TISMIR + JAES |
| Emerging scorer | heuristic rank fusion | bounded ML scorer (logistic regression on embeddings); `ranking_mode=bounded_ml_scorer` in API |

Primary record for 2026-04-25 baseline: [docs/audit/week1-day1-baseline-freeze-2026-04-25.md](docs/audit/week1-day1-baseline-freeze-2026-04-25.md)

ML scorer gate audit: [docs/audit/bundles/production-scoped-shadow-v1/bundle.md](docs/audit/bundles/production-scoped-shadow-v1/bundle.md)

## What Has Been Checked

### Product and API wiring

- Live product routes were captured for Recommended and Evaluation on
  `2026-04-25`.
- Production pins were recorded for `NEXT_PUBLIC_RANKING_VERSION` and
  `NEXT_PUBLIC_EMBEDDING_VERSION`.
- `/readyz` was checked against the Railway API service used by the deployed
  app.
- Evaluation routes for `emerging`, `bridge`, and `undercited` returned the
  pinned run metadata.

Supporting files:

- [docs/audit/week1-day1-baseline-freeze-2026-04-25.md](docs/audit/week1-day1-baseline-freeze-2026-04-25.md)
- [docs/audit/week1-day2-production-pins-2026-04-25.md](docs/audit/week1-day2-production-pins-2026-04-25.md)
- [docs/audit/screenshots/2026-04-25-baseline/](docs/audit/screenshots/2026-04-25-baseline/)

### Automated checks

The repository includes tests for API contracts, ranking behavior, evaluation
comparison, bridge guardrails, and pipeline helpers. CI runs the web build and
Python/API test suite through `npm run validate`.

Useful starting points:

- [apps/api/tests/test_recommendations_ranked.py](apps/api/tests/test_recommendations_ranked.py)
- [apps/api/tests/test_evaluation_compare.py](apps/api/tests/test_evaluation_compare.py)
- [apps/api/tests/test_demo_fixture_mode.py](apps/api/tests/test_demo_fixture_mode.py)
- [services/pipeline/tests/test_ranking_run.py](services/pipeline/tests/test_ranking_run.py)
- [services/pipeline/tests/test_recommendation_review_summary.py](services/pipeline/tests/test_recommendation_review_summary.py)

### No-key demo path

`npm run demo:local` runs the web and API apps against checked-in fixture data.
It is intended to verify setup, route shape, and UI flow without Postgres,
OpenAlex, or OpenAI credentials.

Fixture mode is not live ranking data and should not be cited as model
validation.

## Human Review

### Baseline run (`rank-3904fec89d`)

A complete top-20 single-reviewer pass exists for the April 2026 baseline.

| Family | Rows | P@k good-only | P@k good/acceptable | Bridge-like yes/partial | Surprising/useful |
| --- | ---: | ---: | ---: | ---: | ---: |
| `emerging` | 20 | 1.000 | 1.000 | n/a | 1.000 |
| `bridge` | 20 | 0.900 | 1.000 | 1.000 | 1.000 |
| `undercited` | 20 | 0.700 | 1.000 | n/a | 1.000 |

Supporting files:

- [docs/audit/manual-review/rank-3904fec89d_review_rollup.md](docs/audit/manual-review/rank-3904fec89d_review_rollup.md)
- [docs/audit/manual-review/emerging_rank-3904fec89d_top20_summary.md](docs/audit/manual-review/emerging_rank-3904fec89d_top20_summary.md)
- [docs/audit/manual-review/bridge_rank-3904fec89d_top20_summary.md](docs/audit/manual-review/bridge_rank-3904fec89d_top20_summary.md)
- [docs/audit/manual-review/undercited_rank-3904fec89d_top20_summary.md](docs/audit/manual-review/undercited_rank-3904fec89d_top20_summary.md)

### ML scorer confirmatory surface (`rank-83787b91ef`)

A second-surface confirmatory eval was run on the shadow-generalization snapshot (143 labeled rows, ~38% positive) to assess the bounded ML scorer. Summary:

- P@10 improved from 0.50 (heuristic) to 1.00 (scorer).
- Average precision improved from 0.65 to 0.86.
- This is a single-reviewer offline confirmatory pass, not external validation.

Supporting file: [docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.md](docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.md)

The label dataset behind both surfaces is `ml-label-dataset-v11` (737 labeled rows across families).

How to read all of this:

- Labels are useful directional evidence for specific runs and corpus slices.
- They are not a broad relevance benchmark.
- They are not multi-reviewer agreement evidence.
- They should not be merged with other runs unless a protocol says how.

## Proxy-Only Evaluation

The Evaluation page compares ranked output to citation/date baselines and
reports distributional checks. That helps catch obvious ranking regressions and
keeps the product inspectable, but it is not the same as judged relevance.

Current proxy-evaluation boundary:

- Good for: smoke checks, provenance checks, baseline comparison, and
  regression detection.
- Not enough for: precision/recall claims, semantic-ranking quality claims, or
  production recommender claims.

## Bridge Status

Bridge is still experimental.

Current bridge artifacts support analysis of bridge candidates and an
experimental review arm. It does not support changing defaults or presenting
bridge as a validated recommender.

Supporting files:

- [docs/audit/bridge-evidence-summary.md](docs/audit/bridge-evidence-summary.md)
- [docs/reviewer-brief.md](docs/reviewer-brief.md)

Key boundary:

- The checked-in bridge objective evidence records `ready_for_default=false`.
- Single-reviewer, top-20 offline audit material is not a launch decision.
- Persistent-overlap exclusion is corpus-snapshot-specific and must be
  rederived before any default behavior change.

## What Is Not Claimed Yet

Research Radar does not currently claim:

- A validated recommender system.
- A broad MIR/audio-ML benchmark.
- Multi-reviewer relevance agreement.
- Production-ready bridge weighting.
- General semantic search quality.
- External validation of the live ML scorer.

A trained model (frozen logistic regression on OpenAI embeddings) is deployed
and orders the live Emerging feed when the gate is enabled. That is not the same
as an independently validated recommender. The scorer passed internal holdout and
single-reviewer confirmatory checks only. See the audit bundles for the full gate
trail.

## Next Evaluation Steps

The next credible evaluation work is:

1. **Label bridge negatives.** The bridge surface has 3 labeled rejections in 60
   reviews (95% positive rate) because prior worksheets only sampled the
   heuristic top-20. A new bridge negative-mining worksheet (`rank-83787b91ef`,
   70 rows) is committed and ready to label.
2. **Train and shadow a bridge scorer.** Once ~20–30 labeled bridge rejections
   exist, train an offline bridge scorer (same logistic-regression-on-embeddings
   approach as the current Emerging scorer) and gate it through the same shadow
   ladder.
3. **Expand the corpus.** Add the next source policy rows after their OpenAlex
   source ids and inclusion boundaries are documented.
4. **Improve labeling breadth.** Add multi-reviewer or adjudicated labels for the
   main recommendation families to move past single-reviewer evidence.
5. **Track Emerging scorer v2.** The current scorer was trained on ~125 labeled
   works (v8 dataset). v11 has 390 labeled Emerging rows at 54% positive. A
   retrain is available when warranted.

Planning reference: [docs/eval-foundation-two-week-plan.md](docs/eval-foundation-two-week-plan.md)
