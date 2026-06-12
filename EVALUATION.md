# Evaluation Status

This page is the short status guide for Research Radar. It explains what has
been checked, what is still proxy-only, and what the project does not yet
support as a model-quality claim.

The full experiment record (labeling worksheets, offline eval reports, rollout
review notes) lives on the `archive/ml-governance-audit` branch; this document
keeps only the conclusions.

## Short Version

- Research Radar has working search, recommendations, paper detail, trends, and
  evaluation surfaces backed by versioned ranking runs.
- The public evaluation page compares ranked output against citation/date
  baselines. Those are useful proxy checks, not human relevance validation.
- A complete single-reviewer top-20 labeling pass exists for one pinned baseline
  run across `emerging`, `bridge`, and `undercited`.
- A bounded ML scorer orders the live Emerging feed when its deployment gate is
  enabled; a bounded Bridge hybrid scorer is canary-only.
- Bridge diagnostics are useful for analysis, but bridge is still experimental
  and not default-ready.
- The corpus is small and narrow (TISMIR + JAES), so strong ML benchmark claims
  are deferred until corpus expansion and a larger review protocol.

## Current Live Run vs Archived Baseline

| Reference | What it means | How to use it |
| --- | --- | --- |
| Live app | Current deployed UI and API behavior | Good for interactive review; read the visible run metadata before comparing results |
| Pinned baseline | Frozen `2026-04-25` run stack recorded below | Use for documented claims and stable comparisons |
| URL-pinned run | A specific `ranking_run_id` supplied in the URL or API request | Use when checking one materialized run |
| Fixture mode | Checked-in toy data behind `npm run demo:local` | Setup and route-shape checks only; not ranking validation |

If the live app and the archived baseline differ, treat that as expected.
Record the visible `ranking_run_id`, `ranking_version`,
`corpus_snapshot_version`, and `embedding_version` before drawing conclusions.

## Current Pinned Baseline

The original baseline freeze is dated `2026-04-25`. A second surface was added
in May 2026, and the live Emerging feed now uses a bounded ML scorer on that
surface when the deployment gate is enabled.

| Field | Baseline (2026-04-25) | Live Emerging (2026-05-31) |
| --- | --- | --- |
| `corpus_snapshot_version` | `source-snapshot-20260425-044015` | `source-snapshot-shadow-generalization-v1-20260521` |
| `embedding_version` | `v1-title-abstract-1536-cleantext-r3` | `shadow-generalization-text-embedding-v1` |
| `ranking_version` | `bridge-v2-nm1-zero-r3-k6-20260424` | `shadow-generalization-product-candidate-ranking-v1` |
| `ranking_run_id` | `rank-3904fec89d` | `rank-83787b91ef` |
| Included works | `59` | `528` |
| Source scope | TISMIR + JAES | TISMIR + JAES |
| Emerging scorer | heuristic rank fusion | bounded ML scorer (`ranking_mode=bounded_ml_scorer` in API) |

## What Has Been Checked

- **Product and API wiring.** Live routes, production version pins, and
  `/readyz` were verified against the deployed app on `2026-04-25`.
- **Automated checks.** CI runs the web lint/build and the Python/API test
  suite through `npm run validate`. Useful starting points:
  [apps/api/tests/test_recommendations_ranked.py](apps/api/tests/test_recommendations_ranked.py),
  [apps/api/tests/test_evaluation_compare.py](apps/api/tests/test_evaluation_compare.py),
  [apps/api/tests/test_demo_fixture_mode.py](apps/api/tests/test_demo_fixture_mode.py),
  [services/pipeline/tests/test_ranking_run.py](services/pipeline/tests/test_ranking_run.py).
- **No-key demo path.** `npm run demo:local` runs the web and API apps against
  checked-in fixture data; it verifies setup and route shape only.

## Human Review

### Baseline run (`rank-3904fec89d`)

A complete top-20 single-reviewer pass exists for the April 2026 baseline.

| Family | Rows | P@k good-only | P@k good/acceptable | Bridge-like yes/partial | Surprising/useful |
| --- | ---: | ---: | ---: | ---: | ---: |
| `emerging` | 20 | 1.000 | 1.000 | n/a | 1.000 |
| `bridge` | 20 | 0.900 | 1.000 | 1.000 | 1.000 |
| `undercited` | 20 | 0.700 | 1.000 | n/a | 1.000 |

### Bridge live canary top 20 (`rank-5a7efa5ca3`, reviewed 2026-06-12)

A complete single-reviewer pass over the live canary Bridge top 20 (served
order, `ranking_mode=bounded_bridge_ml_scorer`):

| Rows | P@20 good-only | P@20 good/acceptable | Bridge-like yes/partial | Surprising/useful |
| ---: | ---: | ---: | ---: | ---: |
| 20 | 0.40 | 0.65 | 0.70 | 0.60 |

Reading: the canary surfaces a real bridge signal - the strongest rows are
genuine method-transfer papers (diffusion-based audio restoration,
interpretability on audio foundation models, neural synthesis on embedded
hardware). But the list also includes several isolated or off-domain papers
(humanities essays, an archiving report, a sports-science application) whose
high bridge scores reflect embedding-space isolation rather than connection
between research areas. Verdict: not clean enough for public rollout;
promising enough to continue the canary/private review arm.

### ML scorer confirmatory surface (`rank-83787b91ef`)

A second-surface confirmatory eval (143 labeled rows, ~38% positive) assessed
the bounded Emerging ML scorer: P@10 improved from 0.50 (heuristic) to 1.00
(scorer); average precision improved from 0.65 to 0.86. This is a
single-reviewer offline confirmatory pass, not external validation.

How to read all of this:

- Labels are useful directional evidence for specific runs and corpus slices.
- They are not a broad relevance benchmark or multi-reviewer agreement
  evidence, and should not be merged across runs without a protocol.

## Proxy-Only Evaluation

The Evaluation page compares ranked output to citation/date baselines and
reports distributional checks. That helps catch obvious ranking regressions and
keeps the product inspectable, but it is not judged relevance.

- Good for: smoke checks, provenance checks, baseline comparison, and
  regression detection.
- Not enough for: precision/recall claims, semantic-ranking quality claims, or
  production recommender claims.

## Bridge Status

Bridge is experimental. Current artifacts support analysis of bridge
candidates and an experimental review arm, not default behavior changes.

Summary of the Bridge ML work (full record on the archive branch):

- A frozen logistic-regression Bridge scorer (`v3`, regularized at `C=0.001`
  after an overfitting check: in-sample ROC AUC 1.0 vs out-of-fold ~0.718) was
  trained on 130 deduplicated labeled work ids.
- A rank-percentile hybrid (`0.5 * rank_pct(ml_probability) +
  0.5 * rank_pct(bridge_score)` over the full 528-candidate pool) matched pure
  ML P@20 of 1.0 on the labeled shadow slice, while a linear min-max blend
  degraded precision and was rejected.
- A controlled offline replay on `rank-5a7efa5ca3` passed its risk gates
  (current top-20 precision 0.4 vs proposed hybrid 1.0 on labeled rows).
- A bounded serving gate is implemented and verified end to end: Bridge ML
  serving requires explicit `ML_BRIDGE_SCORER_V1_*` env flags, the pinned run
  `rank-5a7efa5ca3`, and a canary header. As of `2026-06-08` the live cohort
  canary returns `ranking_mode=bounded_bridge_ml_scorer`; public/default
  Bridge requests still return `materialized_heuristic`.

Key boundary: single-reviewer, top-20 offline audit material is not a launch
decision. The live canary top 20 was human-reviewed on `2026-06-12` (see
Human Review above): P@20 good-or-acceptable 0.65, with isolated/off-domain
papers still ranking high on raw bridge score. Public Bridge rollout remains
disabled; the canary/private review arm continues.

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
gate is enabled. That is not the same as an independently validated
recommender; it passed internal holdout and single-reviewer confirmatory
checks only.

## Next Evaluation Steps

1. **Reduce isolation-driven false positives in Bridge.** The canary review
   showed papers that are isolated in embedding space (not connecting two
   areas) still rank high on raw bridge score; address before widening
   exposure.
2. **Label the 7 unlabeled proposed top-20 Bridge papers** if churn review
   warrants.
3. **Expand the corpus** once the next source policy rows are documented.
4. **Improve labeling breadth** with multi-reviewer or adjudicated labels.
5. **Track Emerging scorer v2.** The current scorer was trained on ~125 labeled
   works; newer label datasets support a retrain when warranted.
