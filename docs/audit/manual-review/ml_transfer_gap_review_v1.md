# Transfer gap review worksheet (`ml-transfer-gap-review-v1`)

## Purpose

Reviewer-blank worksheet for transfer and sparse-pool label gaps identified by `ml-production-readiness-plan-v1`. This is label collection infrastructure only: no training, no ranking, no split generation, and no production behavior change.

## Quotas

- **requested rows:** `60`
- **achieved rows:** `45`
- **requested slots:** `{"P1": 21, "P2": 24, "P3": 15}`
- **achieved slots:** `{"P1": 21, "P2": 24, "P3": 0}`
- **shortfall counts:** `{"P1": 0, "P2": 0, "P3": 15}`
- **csv_output:** `docs/audit/manual-review/ml_transfer_gap_review_v1.csv`
- **context_output:** `docs/audit/manual-review/ml_transfer_gap_review_v1_context.json`
- **markdown_output:** `docs/audit/manual-review/ml_transfer_gap_review_v1.md`

## Breakdown By Priority

| priority | rows |
| --- | ---: |
| `P1` | 21 |
| `P2` | 24 |

## Breakdown By Sample Reason

| sample_reason | rows |
| --- | ---: |
| `transfer_gap_external_blind_balance` | 21 |
| `transfer_gap_good_or_acceptable_balance` | 24 |

## Rubric Reminders

- `surprising_or_useful` is deferred for production and needs rubric clarity plus balanced cross-source labels.
- `good_or_acceptable` is research-only and may support future offline ranker research only after gates are addressed.
- P3 rows record `gap_source_pool` only in the sidecar, not in the reviewer CSV.

## Later Ingest Note

When reviewed, save a dated labeled copy such as `ml_transfer_gap_review_v1_labeled_YYYY-MM-DD.csv`. A future v8 ingest should merge this sidecar by `row_id` and keep `review_pool_variant=ml_transfer_gap_audit` distinct.

## Caveats

- This worksheet is not validation.
- Rows are for targeted transfer-gap manual labeling only.
- No model is trained, no ranking is run, and no production ranking change is supported.
- The reviewer CSV intentionally hides score, rank, model, ranking-run, internal database, and gap-source-pool fields.
- surprising_or_useful is deferred for production; good_or_acceptable is research-only.
