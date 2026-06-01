# Bridge negative-mining worksheet (`ml-bridge-negative-mining-v1`)

## Purpose

Reviewer-blind worksheet for bridge-surface negatives and borderline cases drawn below the heuristic top band of a persisted bridge ranking run. This is label-collection only, not model training or validation.

## Provenance

- **worksheet_version:** `ml-bridge-negative-mining-v1`
- **review_pool_variant:** `ml_bridge_negative_mining_audit`
- **sample_seed:** `20260531`
- **ranking_run_id:** `rank-83787b91ef`
- **ranking_version:** `shadow-generalization-product-candidate-ranking-v1`
- **corpus_snapshot_version:** `source-snapshot-shadow-generalization-v1-20260521`
- **embedding_version:** `shadow-generalization-text-embedding-v1`
- **cluster_version:** ``
- **label_dataset:** `docs/audit/ml-label-dataset-v11.json`
- **label_dataset_sha256:** `d3870a97febd92776bf99aecda7b26851e961017614c9bb79010259791c6a328`
- **conflict_policy:** `docs/audit/ml-label-conflict-policy.md`
- **csv_output:** `docs/audit/manual-review/bridge_negative_mining_rank-83787b91ef_v1.csv`
- **context_sidecar_output:** `docs/audit/manual-review/bridge_negative_mining_rank-83787b91ef_v1_context.json`

## Sample Summary

- **requested rows:** `70`
- **achieved rows:** `70`
- **shortfall:** `0`
- **bridge pool size:** `488`
- **top-20 min final_score:** `0.568068`
- **family_rank range:** `[26, 528]`
- **final_score range:** `[-0.2, 0.561339]`

## Row Counts By Sample Reason

| sample_reason | rows |
|---|---:|
| `bridge_deep_cut` | 24 |
| `bridge_suppressed_final` | 23 |
| `corpus_blind_seeded_fill` | 23 |

## Caveats

- This worksheet is not validation of bridge ranking quality.
- Rows are for offline bridge negative / borderline manual labeling only.
- The reviewer CSV intentionally hides ranking scores, ranks, and bridge eligibility flags.
- No bridge model is trained, no ranking is run, and no production bridge change is supported.
