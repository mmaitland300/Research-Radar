# Offline bridge recommendable scorer v2 — combined slice diagnostic

Offline diagnostic model for `bridge_recommendable` trained on both bridge label slices: the negative-mining worksheet (70 rows) and the top-ranked validation worksheet (30 rows). This is not validation and not a serving change.

## Slice

- Rows: 100 (70 negative-mining + 30 top-ranked validation)
- Target true / false: 53 / 47
- Hard negatives: 37
- Bridge-like positive relevance leakage: 0
- Embedding coverage: 100 / 100

## Learned OOF CV — all 100 rows

- ROC AUC: 0.6495383380168608
- Average precision: 0.6399204705796888
- Pairwise accuracy: 0.6495383380168607
- Precision@5 / @10 / @20: 0.6 / 0.7 / 0.65

## Stratified OOF Metrics

| stratum | n | pos | neg | ROC AUC | AP | P@10 |
|---|---:|---:|---:|---:|---:|---:|
| all_100_rows | 100 | 53 | 47 | 0.6495383380168608 | 0.6399204705796888 | 0.7 |
| negative_mining_slice_70_rows | 70 | 38 | 32 | 0.6513157894736843 | 0.6558390276025453 | 0.6 |
| top_ranked_validation_slice_30_rows | 30 | 15 | 15 | 0.6488888888888888 | 0.6258501868501869 | 0.6 |
| top_20_live_bridge_rows | 20 | 8 | 12 | 0.6875 | 0.5617063492063492 | 0.6 |

## Top-20 Live Bridge Comparison (Decision Signal)

- Top-20 rows: 20 (currently-positive: 8, currently-negative: 12)
- Mean OOF probability — positive: 0.5980, negative: 0.3693
- Probability gap (pos − neg): 0.228776
- Pairwise correct-ordering fraction: 0.688 (66/96 pairs)
- **Verdict: model_partially_demotes_top20_bridge_like_no_papers**

Fraction of (bridge_like yes/partial, bridge_like no) pairs in the top-20 live Bridge rows where the model assigns higher probability to the yes/partial paper. >0.75 means the model credibly re-ranks; >0.5 means partial signal; <=0.5 means the model fails to improve on the current live order and needs more labels or features.

## Heuristic Arms

| arm | status | coverage | ROC AUC | AP | P@10 |
|---|---:|---:|---:|---:|---:|
| `final_score` | ok | 100/100 | 0.45343235648334 | 0.5194416361826932 | 0.5 |
| `bridge_score` | not_applicable | 0/100 | None | None | None |
| `semantic_score` | not_applicable | 0/100 | None | None | None |

## Readout

learned_embedding_oof_cv has the highest ROC AUC on this offline diagnostic combined slice. This does not authorize serving or production readiness.

The full-slice fit is included only to freeze diagnostic coefficients; its metrics are in-sample and not validation.

## Caveats

- This is not validation.
- This is a worksheet-selected, two-slice offline diagnostic.
- OOF CV metrics are not in-sample metrics.
- Stratified sub-slice metrics reuse OOF probabilities from the full 100-row CV.
- Top-20 live Bridge comparison is the key decision signal for bridge scorer credibility.
- No ranking, API, DB-write, shadow, or production changes are made or authorized.

Recommended next stage: `collect_more_labels_or_feature_work_before_bridge_serving`.
