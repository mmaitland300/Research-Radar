# Offline bridge hybrid eval v3

Offline diagnostic: C=0.001 v3 OOF probabilities combined with bridge_score on the 60-row shadow-pilot slice. Not validation; no serving change.

## Prerequisite

- Sensitivity artifact: `docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json`
- SHA256: `04a41f91cee1a2a78b7f1f8e9f99b1ef13679a8dacd1b404a848c82751d807d2`
- Selected C: `0.001`

## Shadow slice

- Ranking run: `rank-5a7efa5ca3`
- Rows: `60` (34 pos / 26 neg)
- bridge_score coverage: 60/60

## Arm comparison (primary alpha=0.5)

| arm | ROC AUC | AP | P@10 | P@20 | pairwise |
|---|---:|---:|---:|---:|---:|
| `pure_ml` | 0.8506787330316742 | 0.909336081652395 | 1.0 | 0.95 | 0.8506787330316742 |
| `pure_bridge` | 0.4298642533936652 | 0.5549614219834446 | 0.5 | 0.5 | 0.4298642533936652 |
| `hybrid` | 0.6210407239819005 | 0.7657828067702657 | 1.0 | 0.75 | 0.6210407239819005 |

## Targeted readout verdicts (primary alpha=0.5)

| bucket | arm | verdict / key metric |
|---|---|---|
| `high_bridge_score_low_ml` | `pure_ml` | fails |
| `high_bridge_score_low_ml` | `pure_bridge` | partial |
| `high_bridge_score_low_ml` | `hybrid` | fails |
| `promoted_by_hybrid` | `pure_ml` | maintains_hybrid_promotion |
| `promoted_by_hybrid` | `pure_bridge` | maintains_hybrid_promotion |
| `promoted_by_hybrid` | `hybrid` | maintains_hybrid_promotion |
| `demoted_by_hybrid` | `pure_ml` | separates_demotions |
| `demoted_by_hybrid` | `pure_bridge` | separates_demotions |
| `demoted_by_hybrid` | `hybrid` | separates_demotions |

## Hybrid lift (primary alpha=0.5)

- P@20 delta vs pure_ml: `-0.19999999999999996`
- P@20 delta vs pure_bridge: `0.25`
- hybrid_hurts_ml_precision: `True`

## Recommendation

- **recommended_next_stage:** `do_not_authorize_bridge_hybrid_serving_recheck_alpha_or_formula`
- **hybrid_rescue_confirmed:** `False`
- Primary alpha=0.5 hybrid fails one or more targeted readouts or hurts ML precision; recheck alpha or hybrid formula before serving.

## Caveats

- Offline hybrid eval diagnostic only; does not enable Bridge serving or production output.
- OOF probabilities from the C=0.001 sensitivity sweep, not new scorer inference.
- bridge_score normalization is min-max over the 60-row shadow slice only; production normalization may differ.
- 60-row shadow slice evaluation; not powered for precision on 10-row buckets.
- alpha sweep is diagnostic; no alpha is authorized for production without further review.
- Does not change production default, API/web surface, or user-visible ranking.
- Bridge recommendations remain subject to controlled rollout authorization.
