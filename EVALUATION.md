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
| Emerging scorer | heuristic rank fusion | bounded ML scorer (frozen embedding-model probability + heuristic rank signal); `ranking_mode=bounded_ml_scorer` in API |

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

The label dataset behind these Emerging-focused surfaces is `ml-label-dataset-v12`
(807 labeled rows across families, including the first 70-row bridge
negative-mining slice). Newer Bridge ML diagnostics use `ml-label-dataset-v14`,
which adds top-ranked validation and shadow-pilot disagreement labels.

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

### Bridge ML Diagnostic State (as of 2026-06-06)

Bridge ML evidence has moved past the original negative-mining-only diagnostic.
The current label artifact is `ml-label-dataset-v14`, with
`ml-label-readiness-matrix-v11`. The Bridge audit pool now contains 160
row-level labels across three review variants:

- `ml_bridge_negative_mining_audit`
- `ml_bridge_top_ranked_validation_audit`
- `ml_bridge_shadow_pilot_audit`

After deduplication and source-priority rules, the Bridge v3 scorer trains and
evaluates on 130 unique work ids. This keeps the negative-mining, top-ranked,
and shadow-pilot disagreement evidence in one diagnostic slice without treating
near-duplicate or conflicting rows as independent production validation.

Current Bridge ML findings:

- **Bridge scorer v3:** learned useful bridge-recommendable discrimination from
  OpenAI embedding features, but the original `C=1.0` logistic-regression fit
  overfit the 130-work-id slice. Its in-sample ROC AUC was 1.0 versus OOF ROC
  AUC around 0.718, a gap of about 0.28.
- **Regularization sensitivity:** selected `C=0.001` as the safer frozen
  coefficient setting for offline hybrid evaluation. This frozen scorer is an
  offline-eval artifact, not a production-serving authorization.
- **Historical `rank-83787b91ef`:** `bridge_score` is NULL for all 528 Bridge
  rows in that earlier run, so old bridge diagnostics could only compare ML
  against `final_score`-driven ordering.
- **Current hybrid-eval run `rank-5a7efa5ca3`:** `bridge_score` is populated on
  all 528 Bridge rows, enabling a direct `bridge_score + ML` comparison.

Two hybrid formulas were evaluated on the 60 labeled shadow-pilot rows:

- **Linear min-max blend failed.** The primary `alpha=0.5` formula,
  `alpha * ml_probability + (1-alpha) * normalized_bridge_score`, reduced P@20
  from 0.95 for pure ML to 0.75 for the hybrid. It also failed to reliably
  rescue the `high_bridge_score_low_ml` positives. This formula should not be
  used for Bridge serving.
- **Rank-percentile blend is promising.** The primary `alpha=0.5` formula,
  `alpha * rank_pct(ml_probability) + (1-alpha) * rank_pct(bridge_score)`,
  used full-pool rank percentiles over 528 Bridge candidates and matched pure
  ML P@20 at 1.0 on the labeled shadow slice. Pure `bridge_score` P@20 was 0.5.
  The rank-percentile hybrid also passed the `high_bridge_score_low_ml` targeted
  check with pairwise 0.875.

Important nuance: the rank-percentile hybrid did not beat pure ML on P@20; it
matched it while avoiding the linear blend's precision drop. The same artifact
shows exploratory `alpha=0.7` with stronger ROC AUC and AP than `alpha=0.5`,
but `alpha=0.5` remains the fixed primary arm for that audit.

A controlled offline rollout replay has now been completed on the current
`rank-5a7efa5ca3` Bridge pool. The current Bridge top-20 and proposed
rank-percentile hybrid top-20 had full churn (`20/20`). On labeled rows, current
top-20 precision was 0.4, while the proposed hybrid top-20 precision was 1.0.
The primary `alpha=0.5` risk gates passed: no promoted labeled negatives, no
promoted unlabeled high-risk papers, and no demoted labeled-positive clear
losses. The replay did demote 8 labeled positives, but all were classified as
competitive demotions rather than clear losses.

The proposed hybrid top-20 contains 13 labeled papers and 7 unlabeled papers,
so this supports a bounded Bridge serving gate and reviewable next
implementation step. It does not authorize broad Bridge production rollout,
production default changes, or presenting Bridge as a validated recommender.

The bounded Bridge serving gate has now been implemented. A deployment-readiness
check was performed on 2026-06-07. The configured `DATABASE_URL` checks passed
for `rank-5a7efa5ca3`: the run exists, is succeeded, has 528 Bridge rows, has
`bridge_score` on all 528 rows, and has embeddings for all 528 candidates. The
real serving helper also scored the full pool and returned 20 rows with
`writes_performed=false`. A local API integration smoke verified the gate opens
only with explicit `ML_BRIDGE_SCORER_V1_*` flags and the pinned run, while
default, wrong-limit, eligible-only, wrong-run, cap-exhausted, Emerging, and
Undercited paths fall closed.

Deployment readiness did not pass yet because deployed HTTP checks were not
available: `RESEARCH_RADAR_API_BASE` was not set, the deployed commit was not
confirmed, and live deployed gate-open behavior was not verified. Production
remains fail-closed by default. Bridge ML serving still requires explicit
`ML_BRIDGE_SCORER_V1_*` env flags and pinned `rank-5a7efa5ca3`; controlled
canary or public flag enablement is a next step only after deployed readiness
passes. A secondary follow-up is to label the 7 unlabeled proposed top-20 papers
if churn review warrants.

A deployed-readiness check was then run against the Railway API URL documented
in prior audit notes. Deployed default Bridge behavior is fail-closed, and the
deployed API response schema includes the Bridge scorer fields, but deployed
readiness still failed: the Railway API database resolves Bridge defaults to
`rank-83787b91ef` and returns `404` for the pinned scorer run
`rank-5a7efa5ca3`. A deploy packaging bug was also found and fixed in the API
Dockerfile: the image now copies the Bridge serving plan, sensitivity artifact,
and embeddings provenance JSON needed by gate-open serving. The blocker remains
deployment/data alignment plus a redeploy of that packaging fix. First
enablement, once readiness passes, should be a tiny cohort canary with cap `1`
or `2`, not a public `100%` rollout.

The Railway database was then aligned with the pinned Bridge scorer run:
`rank-5a7efa5ca3` now exists, has 528 Bridge rows, has `bridge_score` populated
on all 528 Bridge rows, and the read-only Bridge serving helper can score the
full pool from Railway Postgres. The deployed pinned request now returns HTTP
`200`, so the missing-run blocker is resolved. Because the diagnostic run is now
visible in Railway, unfiltered API calls can resolve to
`rank-5a7efa5ca3`; Emerging remains correct when callers pass
`ranking_version=shadow-generalization-product-candidate-ranking-v1`, which is
the expected Vercel page behavior. A subsequent web/API routing fix made Bridge
pinning explicit without moving Emerging off its product run.

#### Live Bridge Canary (as of 2026-06-08)

The first live Bridge canary proof attempt was intentionally recorded as a
failed artifact: the public/default Bridge request stayed
`materialized_heuristic`, and the cohort canary request also stayed
`materialized_heuristic`. The failure was traced to bounded canary exposure
state rather than a model or data blocker.

After redeploying Railway with a higher internal canary cap, the live cohort
canary request with
`X-Research-Radar-Canary-Subject: bridge-deploy-readiness-v1` returned HTTP
`200` with `ranking_mode=bounded_bridge_ml_scorer`,
`bridge_recommendations_ml_served=true`, `scorer_surface=bridge`,
`bridge_rank_pct_hybrid_alpha=0.5`, and
`bridge_rank_pct_scope=full_bridge_candidate_pool`. The response resolved
`rank-5a7efa5ca3`, returned 20 items, and kept
`emitted_to_public_users=false`.

Public/default Bridge requests without the canary header still return
`materialized_heuristic`, so public rollout remains disabled. The next step is
human review of the live canary top 20, not broad rollout.

Supporting files:

- [docs/audit/ml-offline-bridge-recommendable-scorer-v1.json](docs/audit/ml-offline-bridge-recommendable-scorer-v1.json)
- [docs/audit/ml-offline-bridge-recommendable-scorer-v1.md](docs/audit/ml-offline-bridge-recommendable-scorer-v1.md)
- [docs/audit/ml-offline-bounded-hybrid-bridge-eval-v1.json](docs/audit/ml-offline-bounded-hybrid-bridge-eval-v1.json)
- [docs/audit/ml-offline-bounded-hybrid-bridge-eval-v1.md](docs/audit/ml-offline-bounded-hybrid-bridge-eval-v1.md)
- [docs/audit/ml-offline-bridge-recommendable-scorer-v3.json](docs/audit/ml-offline-bridge-recommendable-scorer-v3.json)
- [docs/audit/ml-offline-bridge-recommendable-scorer-v3.md](docs/audit/ml-offline-bridge-recommendable-scorer-v3.md)
- [docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json](docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json)
- [docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.md](docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.md)
- [docs/audit/ml-offline-bridge-hybrid-eval-v3-v1.json](docs/audit/ml-offline-bridge-hybrid-eval-v3-v1.json)
- [docs/audit/ml-offline-bridge-hybrid-eval-v3-v1.md](docs/audit/ml-offline-bridge-hybrid-eval-v3-v1.md)
- [docs/audit/ml-offline-bridge-hybrid-rank-pct-eval-v3-v1.json](docs/audit/ml-offline-bridge-hybrid-rank-pct-eval-v3-v1.json)
- [docs/audit/ml-offline-bridge-hybrid-rank-pct-eval-v3-v1.md](docs/audit/ml-offline-bridge-hybrid-rank-pct-eval-v3-v1.md)
- [docs/audit/ml-bridge-rank-pct-hybrid-controlled-rollout-eval-v1.json](docs/audit/ml-bridge-rank-pct-hybrid-controlled-rollout-eval-v1.json)
- [docs/audit/ml-bridge-rank-pct-hybrid-controlled-rollout-eval-v1.md](docs/audit/ml-bridge-rank-pct-hybrid-controlled-rollout-eval-v1.md)
- [docs/audit/ml-bridge-rank-pct-hybrid-serving-plan-v1.json](docs/audit/ml-bridge-rank-pct-hybrid-serving-plan-v1.json)
- [docs/audit/ml-bridge-rank-pct-hybrid-serving-plan-v1.md](docs/audit/ml-bridge-rank-pct-hybrid-serving-plan-v1.md)
- [docs/audit/bridge-scorer-deployment-readiness-v1.json](docs/audit/bridge-scorer-deployment-readiness-v1.json)
- [docs/audit/bridge-scorer-deployment-readiness-v1.md](docs/audit/bridge-scorer-deployment-readiness-v1.md)
- [docs/audit/bridge-scorer-deployed-readiness-v1.json](docs/audit/bridge-scorer-deployed-readiness-v1.json)
- [docs/audit/bridge-scorer-deployed-readiness-v1.md](docs/audit/bridge-scorer-deployed-readiness-v1.md)
- [docs/audit/bridge-scorer-railway-data-alignment-v1.json](docs/audit/bridge-scorer-railway-data-alignment-v1.json)
- [docs/audit/bridge-scorer-railway-data-alignment-v1.md](docs/audit/bridge-scorer-railway-data-alignment-v1.md)
- [docs/audit/bridge-scorer-live-canary-proof-v1.json](docs/audit/bridge-scorer-live-canary-proof-v1.json)
- [docs/audit/bridge-scorer-live-canary-proof-v1.md](docs/audit/bridge-scorer-live-canary-proof-v1.md)
- [docs/audit/bridge-scorer-live-canary-proof-v2.json](docs/audit/bridge-scorer-live-canary-proof-v2.json)
- [docs/audit/bridge-scorer-live-canary-proof-v2.md](docs/audit/bridge-scorer-live-canary-proof-v2.md)

## What Is Not Claimed Yet

Research Radar does not currently claim:

- A validated recommender system.
- A broad MIR/audio-ML benchmark.
- Multi-reviewer relevance agreement.
- Production-ready bridge weighting.
- General semantic search quality.
- External validation of the live ML scorer.

A bounded hybrid scorer (frozen logistic regression on OpenAI embeddings plus
heuristic rank signal) is deployed and orders the live Emerging feed when the
gate is enabled. That is not the same as an independently validated recommender.
The scorer passed internal holdout and single-reviewer confirmatory checks only.
See the audit bundles for the full gate trail.

## Next Evaluation Steps

The next credible evaluation work is:

1. **Human-review the live Bridge canary top 20.** The canary path now returns
   `bounded_bridge_ml_scorer`, but the result is still internal API-operator
   evidence. Review the top 20 for quality, off-topic papers, and misleading
   Bridge matches before widening exposure.
2. **Label the 7 unlabeled proposed top-20 Bridge papers if churn review
   warrants.** The controlled replay supports a bounded gate, but the unlabeled
   proposed rows remain the most useful targeted review follow-up.
3. **Expand the corpus.** Add the next source policy rows after their OpenAlex
   source ids and inclusion boundaries are documented.
4. **Improve labeling breadth.** Add multi-reviewer or adjudicated labels for the
   main recommendation families to move past single-reviewer evidence.
5. **Track Emerging scorer v2.** The current scorer was trained on ~125 labeled
   works (v8 dataset). v12 has additional Emerging rows at 54% positive. A
   retrain is available when warranted.

Planning reference: [docs/eval-foundation-two-week-plan.md](docs/eval-foundation-two-week-plan.md)
