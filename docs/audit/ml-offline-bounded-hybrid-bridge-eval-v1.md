# Offline bounded hybrid bridge eval v1

Offline diagnostic only: evaluates bounded rank-mean hybrids for `bridge_recommendable` on `review_pool_variant=ml_bridge_negative_mining_audit`. This is not validation and not a serving change.

- Rank percentile scope: `labeled_slice_only`
- Rows: 70
- Target true / false: 38 / 32
- Primary fixed arm: `hybrid_rank_mean_50_50`

## Arms

| arm | ROC AUC | AP | Pairwise | P@5 | P@10 | P@20 | top20 positives |
|---|---:|---:|---:|---:|---:|---:|---:|
| `heuristic_final_score` | 0.4609375 | 0.5535052167225342 | 0.4609375 | 0.6 | 0.6 | 0.5 | 10 |
| `learned_bridge_probability_oof` | 0.6866776315789473 | 0.6751724203178869 | 0.6866776315789473 | 0.6 | 0.6 | 0.75 | 15 |
| `hybrid_rank_mean_50_50` | 0.6036184210526315 | 0.6161435210004722 | 0.6036184210526315 | 0.8 | 0.5 | 0.55 | 11 |
| `hybrid_rank_mean_70_30_heuristic` | 0.5386513157894737 | 0.5821247772874952 | 0.5386513157894737 | 0.8 | 0.4 | 0.5 | 10 |
| `hybrid_rank_mean_30_70_heuristic` | 0.6578947368421053 | 0.6539458382773077 | 0.6578947368421053 | 0.4 | 0.7 | 0.6 | 12 |

## Readout

- Best arm by ROC AUC: `learned_bridge_probability_oof` = `0.6866776315789473` (exploratory only).
- Best arm by average precision: `learned_bridge_probability_oof` = `0.6751724203178869` (exploratory only).
- Recommended next stage: `do_not_combine_signals_collect_labels_or_fix_features`

The primary arm is fixed as `hybrid_rank_mean_50_50`; best-arm selection is exploratory only and does not authorize production serving.

## Caveats

- This is not validation.
- This is a single-reviewer, worksheet-selected 70-row slice.
- Rank percentiles are labeled_slice_only and are not full-pool production scores.
- Labeled-row metrics use bridge scorer OOF probabilities only.
- Best-arm readout is exploratory only.
- No DB writes, ranking writes, serving changes, or production authorization are made.
