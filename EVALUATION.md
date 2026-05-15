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

The latest checked-in baseline freeze is dated `2026-04-25`.

| Field | Value |
| --- | --- |
| `corpus_snapshot_version` | `source-snapshot-20260425-044015` |
| `embedding_version` | `v1-title-abstract-1536-cleantext-r3` |
| `cluster_version` | `kmeans-l2-v0-cleantext-r3-k6` |
| `ranking_version` | `bridge-v2-nm1-zero-r3-k6-20260424` |
| `ranking_run_id` | `rank-3904fec89d` |
| Included works | `59` |
| Source scope | TISMIR + JAES |

Primary record: [docs/audit/week1-day1-baseline-freeze-2026-04-25.md](docs/audit/week1-day1-baseline-freeze-2026-04-25.md)

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

A complete top-20 single-reviewer pass exists for the pinned baseline run
`rank-3904fec89d`.

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

How to read this:

- The labels are useful directional evidence for one run and one corpus slice.
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
- Custom model training readiness.

Those require a larger corpus, a fixed review protocol, and stronger evaluation
artifacts than the current smoke/demo evidence.

## Next Evaluation Steps

The next credible evaluation work is:

1. Expand the corpus beyond the current small TISMIR + JAES slice.
2. Cut a new snapshot and rerun embeddings, clustering, and ranking.
3. Define a stable labeling protocol before benchmark-style claims.
4. Add multi-reviewer or adjudicated labels for the main recommendation
   families.
5. Compare new ranking variants against the pinned baseline with documented
   success/failure criteria.

Planning reference: [docs/eval-foundation-two-week-plan.md](docs/eval-foundation-two-week-plan.md)
