# Offline bridge recommendable scorer v1

Offline diagnostic model for `bridge_recommendable` trained only on `review_pool_variant=ml_bridge_negative_mining_audit`. This is not validation and not a serving change.

## Slice

- Rows: 70
- Target true / false: 38 / 32
- Hard negatives: 22
- Bridge-like positive relevance leakage: 0
- Embedding coverage: 70 / 70

## Learned OOF CV

- ROC AUC: 0.6866776315789473
- Average precision: 0.6751724203178869
- Pairwise accuracy: 0.6866776315789473
- Precision@5 / @10 / @20: 0.6 / 0.6 / 0.75

## Heuristic Arms

| arm | status | coverage | ROC AUC | AP | P@10 |
|---|---:|---:|---:|---:|---:|
| `final_score` | ok | 70/70 | 0.4609375 | 0.5535052167225342 | 0.6 |
| `bridge_score` | not_applicable | 0/70 | None | None | None |
| `semantic_score` | not_applicable | 0/70 | None | None | None |

## Readout

learned_embedding_oof_cv has the highest ROC AUC on this offline diagnostic slice. This does not authorize serving or production readiness.

The full-slice fit is included only to freeze diagnostic coefficients; its metrics are in-sample and not validation.

## Caveats

- This is not validation.
- This is a worksheet-selected, single-reviewer offline diagnostic slice.
- Global v12 has overlapping paper_ids with conflicting labels in other pools; training is valid only under the slice filter.
- OOF CV metrics are not in-sample metrics.
- Beating final_score on this slice does not authorize serving.
- No ranking, API, DB-write, shadow, or production changes are made or authorized.

Recommended next stage: `offline_bounded_hybrid_bridge_eval_v1`.
