# Bridge top-ranked validation worksheet (`ml-bridge-top-ranked-v1`)

## Purpose

Label-collection worksheet targeting the actual top-ranked Bridge papers on the `rank-83787b91ef` run, plus a contrastive borderline slice just below the top-20 cut. Complements the negative-mining worksheet (rank 26-528) with the visible Bridge surface.

## bridge_score Coverage Report

- **ranking_run_id:** `rank-83787b91ef`
- **total bridge rows:** `528`
- **bridge_score null:** `528`
- **bridge_score zero:** `0`
- **bridge_score nonzero:** `0`
- **coverage:** `0.0%`

> **Finding:** bridge_score is NULL for all 528 bridge rows in rank-83787b91ef. Bridge ranking is driven entirely by final_score on this run. A direct bridge_score + ML hybrid comparison is not possible until a run with populated bridge_score is available.

This means the previous bounded-hybrid evaluation could not test `bridge_score + ML hybrid` on this run. Any future hybrid comparison requires a run with populated `bridge_score`.

## Provenance

- **worksheet_version:** `ml-bridge-top-ranked-v1`
- **review_pool_variant:** `ml_bridge_top_ranked_validation_audit`
- **sample_seed:** `20260601`
- **ranking_run_id:** `rank-83787b91ef`
- **ranking_version:** `shadow-generalization-product-candidate-ranking-v1`
- **corpus_snapshot_version:** `source-snapshot-shadow-generalization-v1-20260521`
- **embedding_version:** `shadow-generalization-text-embedding-v1`
- **cluster_version:** `(none)`
- **label_dataset:** `docs/audit/ml-label-dataset-v12.json`
- **csv_output:** `docs/audit/manual-review/bridge_top_ranked_rank-83787b91ef_v1.csv`
- **context_sidecar_output:** `docs/audit/manual-review/bridge_top_ranked_rank-83787b91ef_v1_context.json`

## Sample Summary

- **total rows:** `30`
- **top_ranked rows (rank 1-20):** `20`
- **contrastive_borderline rows (rank 21-40):** `10`
- **contrastive pool available:** `13`
- **already-labeled on this run excluded from contrastive:** `70`
- **top-ranked final_score range:** `[0.568068, 0.671]`

## Row Counts By Sample Reason

| sample_reason | rows |
|---|---:|
| `bridge_top_ranked` | 20 |
| `bridge_borderline_contrastive` | 10 |

## Caveats

- This worksheet is for offline label collection only.
- Top-ranked rows surface what is currently live on the Bridge feed.
- Borderline-contrastive rows are just below the top-20 cut and not previously labeled.
- bridge_score is 0/528 non-null for rank-83787b91ef; ranking in this run is final_score-only.
- No bridge model is trained, no ranking is modified, and no production change is implied.
