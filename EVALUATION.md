# Evaluation Status

This public status guide keeps the review path short: what has been checked,
what is still proxy-only, and what the project does not claim.

For the June 2026 curation marker, see
[docs/releases/v0.2.0.md](docs/releases/v0.2.0.md). The full historical
experiment record lives on the `archive/ml-governance-audit` branch. Internal Bridge canary notes: [docs/internal/bridge-canary-review.md](docs/internal/bridge-canary-review.md).

## Short Version

- Research Radar has working search, recommendations, paper detail, trends, and
  evaluation surfaces backed by versioned ranking runs.
- The public evaluation page compares ranked output against citation/date
  baselines. Those are useful proxy checks, not human relevance validation.
- One complete single-reviewer top-20 labeling pass exists for the April 2026
  pinned baseline across `emerging`, `bridge`, and `undercited`.
- The live Emerging feed can use a bounded ML scorer when gated; this is
  internal evidence, not external validation.
- Bridge is experimental: single-reviewer evidence only, canary/private review
  arm only, and not default-ready.
- The corpus is intentionally narrow, so broad benchmark claims are deferred.

## Current Evidence Boundary

| Evidence | Use it for | Boundary |
| --- | --- | --- |
| Live app | Interactive review of the current UI/API and visible run metadata | Moving surface; capture run ids before comparing |
| Pinned baseline | Stable April 2026 review reference | Single-reviewer evidence on one narrow corpus slice |
| URL-pinned run | Inspecting one materialized `ranking_run_id` | Run-specific, not a general benchmark |
| Fixture mode | Local setup and route-shape checks | Toy data; not ranking validation |
| Archived experiment record | Historical worksheets, offline evals, and rollout notes | Stored on `archive/ml-governance-audit`, not the public review path |

## Pinned Baseline And Live Reference

| Field | Pinned baseline | Live Emerging reference |
| --- | --- | --- |
| Date | 2026-04-25 | 2026-05-31 |
| `corpus_snapshot_version` | `source-snapshot-20260425-044015` | `source-snapshot-shadow-generalization-v1-20260521` |
| `embedding_version` | `v1-title-abstract-1536-cleantext-r3` | `shadow-generalization-text-embedding-v1` |
| `ranking_version` | `bridge-v2-nm1-zero-r3-k6-20260424` | `shadow-generalization-product-candidate-ranking-v1` |
| `ranking_run_id` | `rank-3904fec89d` | `rank-83787b91ef` |
| Included works | 59 | 528 |
| Source scope | TISMIR + JAES | TISMIR + JAES |
| Emerging mode | heuristic rank fusion | bounded ML scorer when gated |

## Human Review Summary

All rows below are single-reviewer checks. They are useful directional evidence
for specific runs and corpus slices, not a broad relevance benchmark.

| Surface | Run | Rows | Good-only P@k | Good/acceptable P@k | Notes |
| --- | --- | ---: | ---: | ---: | --- |
| Emerging baseline | `rank-3904fec89d` | 20 | 1.00 | 1.00 | April 2026 pinned baseline |
| Bridge baseline | `rank-3904fec89d` | 20 | 0.90 | 1.00 | Diagnostic bridge evidence only |
| Undercited baseline | `rank-3904fec89d` | 20 | 0.70 | 1.00 | Low-cite candidate slice |
| Bridge canary | `rank-5a7efa5ca3` | 20 | 0.40 | 0.65 | Private/canary review arm; not default-ready |
| Emerging ML scorer | `rank-83787b91ef` | 143 labeled rows | P@10 1.00 | n/a | Offline confirmatory pass; not external validation |

## Proxy-Only Evaluation

The Evaluation page compares ranked output to citation/date baselines and
reports distributional checks. That helps catch obvious regressions and keeps
ranking behavior inspectable.

- Good for: smoke checks, provenance checks, baseline comparison, and
  regression detection.
- Not enough for: precision/recall claims, semantic-ranking quality claims, or
  production recommender claims.

## What Is Not Claimed

Research Radar does not currently claim:

- A validated recommender system.
- A broad MIR/audio-ML benchmark.
- Multi-reviewer relevance agreement.
- Default-ready Bridge ranking.
- General semantic search quality.
- External validation of the live ML scorer.

## Next Evaluation Steps

1. Expand source-policy coverage before treating Bridge results as product
   evidence.
2. Label the remaining proposed Bridge top-20 papers if churn review warrants.
3. Expand the corpus after source policy rows are documented.
4. Improve labeling breadth with multi-reviewer or adjudicated labels.
5. Revisit the Emerging scorer when the newer label datasets justify a retrain.
