# Bridge eligibility failure analysis

- **baseline_minimum** `variant_vs_emerging_jaccard`: `0.212121`
- **tied_variants_with_same_top_20_count:** `3`
- **primary_suspected_cause:** `tied_variants_collapse_to_same_top_20`

## Tied variants (at baseline minimum)

| variant_id | eligible_count_total | top_k_hash (sha256, first 12 hex) | eligible_top_k ∩ emerging |
| --- | ---: | --- | --- |
| `existing_bridge_eligible` | 93 | `1a7e25b9d7d5...` | `[10, 14, 21, 104, 125, 131, 138]` |
| `bridge_score_top_25pct` | 54 | `808cdbb3ec5e...` | `[10, 14, 116, 125, 128, 131, 138]` |
| `top50_and_cross_cluster_gte_0_40` | 92 | `1a7e25b9d7d5...` | `[10, 14, 21, 104, 125, 131, 138]` |
| `top50_and_same_cluster_lte_0_60` | 92 | `1a7e25b9d7d5...` | `[10, 14, 21, 104, 125, 131, 138]` |
| `top25_and_cross_cluster_gte_0_40` | 52 | `f0d4fac07f71...` | `[10, 14, 116, 125, 128, 131, 138]` |

## Persistent / union overlap (tied variants only)

- **persistent_shared_with_emerging_ids:** `[10, 14, 125, 131, 138]`
- **union_shared_with_emerging_ids_across_tied_variants:** `[10, 14, 21, 104, 116, 125, 128, 131, 138]`

### Per work_id: count of tied variants where id appears in (eligible_top_k ∩ emerging)

```json
{
  "10": 5,
  "14": 5,
  "21": 3,
  "104": 3,
  "116": 2,
  "125": 5,
  "128": 2,
  "131": 5,
  "138": 5
}
```

## Cohort cost (variant_vs_emerging_jaccard < 0.40)

Sorted by Jaccard ascending. Rows with `eligible_count_total` < 50 are cohort-collapse risk.

| variant_id | eligible_count_total | variant_vs_emerging_jaccard |
| --- | ---: | ---: |
| `bridge_score_top_25pct` | 54 | 0.212121 |
| `existing_bridge_eligible` | 93 | 0.212121 |
| `top25_and_cross_cluster_gte_0_40` | 52 | 0.212121 |
| `top50_and_cross_cluster_gte_0_40` | 92 | 0.212121 |
| `top50_and_same_cluster_lte_0_60` | 92 | 0.212121 |
| `bridge_score_top_50pct` | 108 | 0.25 |

_No variants in the <0.40 Jaccard band have eligible_count_total below 50._

## Supporting signals (from bundled JSON)

- **bridge_score percentile Jaccards (75 / 50 / 25 pct):** `{'bridge_score_top_75pct': 0.428571, 'bridge_score_top_50pct': 0.25, 'bridge_score_top_25pct': 0.212121}`
- **Monotonic distinctness loss when loosening bridge_score cutoff:** `True`
- **Pearson(bridge_score, final_score) on diagnostics `bridge_top_k_rows`:** `-0.12147045419452576`

## Why Jaccard cannot drop below the baseline minimum on this sweep

Several eligibility rewrites tie at the same minimum Jaccard because they either reproduce the same eligible-only top-20 as `existing_bridge_eligible`, or they reshuffle lower-ranked eligible mass without removing the persistent intersection with `emerging_top_k`. Any rule that keeps the same seven emerging hits in the eligible top-20 reproduces the same Jaccard (7 / (20+20-7)). Tightening bridge_score alone eventually retrains mass into that same floor unless emerging-adjacent works are explicitly down-ranked or excluded.

## Recommended next lever

Redefine the bridge objective to penalize high final_score(emerging) (for example, bridge_eligible := cross_cluster_share >= X AND work_id NOT IN emerging_top_k_50); do not pick a threshold variant from this sweep.

## Verbatim caveats

> Diagnostic only; not validation; no ranking/default/product claim.
> No new eligibility rule should be selected without a follow-up zero-weight ranking run and labels.
