# Bridge Canary Review Notes

Internal context for the public evaluation status guide. This note preserves the
detailed Bridge canary and scorer chronology that used to live in
`EVALUATION.md`.

The full experiment record, including worksheets, offline eval dumps, and
rollout notes, lives on the `archive/ml-governance-audit` branch. This document
is a compact narrative summary, not a new validation claim.

## Current Boundary

Bridge remains experimental. Current evidence supports analysis and a
private/canary review arm only:

- single-reviewer evidence,
- top-20 and labeled-slice evidence,
- canary/private review path only,
- not public/default-ready.

## Bridge Live Canary Top 20

Run: `rank-5a7efa5ca3`

Review date: 2026-06-12

Surface: served Bridge canary order with
`ranking_mode=bounded_bridge_ml_scorer`.

| Rows | P@20 good-only | P@20 good/acceptable | Bridge-like yes/partial | Surprising/useful |
| ---: | ---: | ---: | ---: | ---: |
| 20 | 0.40 | 0.65 | 0.70 | 0.60 |

Reading: the canary surfaced some genuine method-transfer papers, including
diffusion-based audio restoration, interpretability on audio foundation models,
and neural synthesis on embedded hardware. It also surfaced several off-domain
papers, including humanities essays, an archiving report, and a sports-science
application.

A follow-up check against labeled rows found that every false positive came
from the unpoliced portion of the candidate pool: 407 of 528 works had no
source-policy row. All six rows from policy-covered venues were labeled good or
acceptable. Embedding-space isolation measures such as centroid distance,
margin, and nearest neighbor did not separate the false positives.

Conclusion: the signal is promising enough to keep a private/canary review arm,
but not clean enough for public/default Bridge rollout.

## Bridge ML Scorer Chronology

- A frozen logistic-regression Bridge scorer (`v3`) was regularized at
  `C=0.001` after an overfitting check: in-sample ROC AUC 1.0 versus
  out-of-fold about 0.718. It was trained on 130 deduplicated labeled work ids.
- A rank-percentile hybrid was selected:
  `0.5 * rank_pct(ml_probability) + 0.5 * rank_pct(bridge_score)` over the full
  528-candidate pool.
- The rank-percentile hybrid matched pure ML P@20 of 1.0 on the labeled shadow
  slice. A linear min-max blend degraded precision and was rejected.
- A controlled offline replay on `rank-5a7efa5ca3` passed its risk gates:
  current top-20 precision 0.4 versus proposed hybrid 1.0 on labeled rows.
- A bounded serving gate was implemented for review only. Bridge ML serving
  requires explicit `ML_BRIDGE_SCORER_V1_*` env flags, the pinned run
  `rank-5a7efa5ca3`, and a canary header.
- As of 2026-06-08, the live cohort canary returned
  `ranking_mode=bounded_bridge_ml_scorer`; public/default Bridge requests still
  returned `materialized_heuristic`.

## Emerging ML Scorer Context

Run: `rank-83787b91ef`

A second-surface confirmatory eval assessed the bounded Emerging ML scorer on
143 labeled rows, about 38% positive:

| Metric | Heuristic | Scorer |
| --- | ---: | ---: |
| P@10 | 0.50 | 1.00 |
| Average precision | 0.65 | 0.86 |

This was a single-reviewer offline confirmatory pass, not external validation.

## Current Decision

Public Bridge rollout remains disabled. The current next step is source-policy
coverage, not broader exposure. All canary false positives came from works
outside policy-covered venues, but a hard venue filter would also remove some
of the best off-venue bridge finds. The better follow-up is to add policy rows
for more venues, with ISMIR as the next obvious candidate.
