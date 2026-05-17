# Learned Scorer Holdout Policy (ml-learned-scorer-holdout-policy-v1)

## Executive Summary

This policy defines an independent evaluation boundary for future learned audit embedding scorers. It is policy only: no train/eval assignment rows are written, no scorer is trained, and no product-candidate scoring is rerun.

- **Eligible target:** `good_or_acceptable`
- **Primary strategy:** `product_candidate_snapshot_holdout`
- **Eval work count:** 217
- **Eval work-set SHA256:** `213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a`
- **Shadow/prod:** blocked

## Why V2 Gates Require This Policy

The v2 learned metrics showed that the frozen audit scorer could be applied to the product-candidate labeled overlap, but the scorer was full-fit on the audit-labeled corpus. Because the product-candidate overlap uses the same label dataset and embedding rows, those metrics are application diagnostics, not independent validation.

## Primary Holdout Strategy

`product_candidate_snapshot_holdout` is selected for v1. Product-candidate snapshot works become the reserved eval work set for the next holdout-bound scorer chain.

## Train Vs Eval Definitions

- **Eval:** all unique canonical works in candidate_pool_rows, not only labeled works.
- **Train:** all audit-eligible canonical works from v8/v3 embeddings whose canonical work ID is not in eval_work_set.
- All observations for one canonical work must share one future assignment.
- No eval work may be used to fit the holdout-bound scorer.

## Product-Candidate Eval Work-Set Source

- **Source artifact:** `ml-offline-production-candidate-scoring-v2`
- **Source field:** `candidate_pool_rows[].canonical_openalex_work_id`
- **Ranking run:** `rank-ee2ba6c816`
- **Family:** `emerging`

## Leakage Rules

- No canonical OpenAlex work may appear in both train and eval.
- The eval work set is all unique product-candidate snapshot works, not only labeled works.
- Label-based metrics may only use eval observations with explicit boolean labels.

## Conflict/Duplicate Handling

- Silent label merge is not allowed.
- Duplicate work groups are assigned as a unit.
- Observation-level rows are preserved.

## Dataset Inventory Summary

| Metric | Count |
| --- | ---: |
| Audit-eligible observations | 427 |
| Audit-eligible unique works | 342 |
| Audit-eligible observations with embeddings | 427 |
| Audit-eligible works with embeddings | 342 |
| Product-candidate eval works | 217 |
| Product-candidate labeled eval works | 217 |
| Product-candidate unlabeled eval works | 0 |
| Train work estimate | 125 |
| Train observation estimate | 141 |
| Eval observation estimate | 286 |
| Eval positive observations | 255 |
| Eval negative observations | 31 |
| Duplicate work groups | 55 |
| Duplicate observation pressure | 85 |
| Conflicting target work groups | 3 |
| Full-fit/eval overlap fixed by this policy | 217 |

## Future Command Chain

- `ml-learned-scorer-holdout-assignment-v1`: materialize per-work and per-row train/eval assignments
- `ml-offline-audit-embedding-scorer-export-v2`: train only on train-arm observations
- `ml-offline-production-candidate-scoring-v3`: apply holdout-bound scorer to product-candidate eval arm
- `ml-offline-production-candidate-metric-gates-v3`: evaluate held-out learned metrics

## What Still Blocks Shadow

- Independent validation complete: False
- Missing holdout assignment: True
- Missing holdout-bound scorer export: True
- Missing product-candidate scoring v3: True
- Missing metric gates v3: True
- Missing `ml-shadow-scorer-v1`: True
- Shadow scoring authorized: False
- Production default authorized: False

## Not Validation / Not Production Caveats

- Not validation.
- Policy only; no assignments yet.
- Single-reviewer audit labels.
- Product snapshot is one ranking run/family, not live recommender quality.
- The eval work set is reserved from training, but label-based metrics require labeled observations.
- Observation-level duplicates/conflicts are preserved.
- No ranking/API/web changes.
- No shadow or production default authorization.
