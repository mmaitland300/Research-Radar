# Offline bridge hybrid rank-percentile eval v3

Offline diagnostic: rank-percentile blend of v3 ML + bridge_score on the shadow-pilot labeled slice. Not validation; no serving change.

## Scope

- rank_percentile_scope: `full_bridge_candidate_pool`
- pool_candidate_count: `528`
- ml_probability_source_for_rank_pct: `frozen_v3_C0_001_full_fit`
- hybrid_formula: `alpha * rank_pct(ml_probability) + (1-alpha) * rank_pct(bridge_score)`
- rank_pct_method: `shadow_pilot_average_rank_over_n`

## Prerequisite

- Sensitivity artifact: `docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json`
- SHA256: `04a41f91cee1a2a78b7f1f8e9f99b1ef13679a8dacd1b404a848c82751d807d2`
- Selected C: `0.001`

## Shadow slice

- Ranking run: `rank-5a7efa5ca3`
- Rows: `60` (34 pos / 26 neg)
- bridge_score coverage: 60/60

## Arm comparison (primary alpha=0.5, rank percentiles)

| arm | ROC AUC | AP | P@10 | P@20 | pairwise |
|---|---:|---:|---:|---:|---:|
| `pure_ml` | 0.9932126696832579 | 0.9955882352941177 | 1.0 | 1.0 | 0.9932126696832579 |
| `pure_bridge` | 0.4298642533936652 | 0.5549614219834446 | 0.5 | 0.5 | 0.4298642533936652 |
| `hybrid` | 0.8970588235294118 | 0.9341772794976577 | 1.0 | 1.0 | 0.8970588235294118 |

## Targeted readout verdicts (primary alpha=0.5)

| bucket | arm | verdict / key metric |
|---|---|---|
| `high_bridge_score_low_ml` | `pure_ml` | rescues_high_bridge_positives |
| `high_bridge_score_low_ml` | `pure_bridge` | partial |
| `high_bridge_score_low_ml` | `hybrid` | rescues_high_bridge_positives |
| `promoted_by_hybrid` | `pure_ml` | maintains_hybrid_promotion |
| `promoted_by_hybrid` | `pure_bridge` | maintains_hybrid_promotion |
| `promoted_by_hybrid` | `hybrid` | maintains_hybrid_promotion |
| `demoted_by_hybrid` | `pure_ml` | separates_demotions |
| `demoted_by_hybrid` | `pure_bridge` | separates_demotions |
| `demoted_by_hybrid` | `hybrid` | separates_demotions |

## Hybrid lift (primary alpha=0.5)

- P@20 delta vs pure_ml: `0.0`
- P@20 delta vs pure_bridge: `0.5`
- hybrid_hurts_ml_precision: `False`

## Recommendation

- **recommended_next_stage:** `authorize_bridge_hybrid_serving_controlled_rollout_eval`
- **hybrid_rescue_confirmed:** `True`
- Primary alpha=0.5 rank-percentile hybrid rescues high-bridge positives, maintains promoted bucket signal, separates demotion subgroups, and does not materially hurt ML P@20.

## Caveats

- Offline rank-percentile hybrid eval diagnostic only; does not enable Bridge serving.
- Rank percentiles use the shadow-pilot formula (1 - average_rank/n), not min-max linear blend.
- Full-pool scope scores all Bridge candidates with frozen v3 C=0.001 inference (not OOF).
- Labeled-slice-only scope computes rank percentiles over the 60 shadow rows only.
- 60-row labeled evaluation; not powered for precision on 10-row buckets.
- Alpha sweep is diagnostic; no alpha is authorized for production without further review.
- Does not change production default, API/web surface, or user-visible ranking.
- Bridge recommendations remain subject to controlled rollout authorization.
