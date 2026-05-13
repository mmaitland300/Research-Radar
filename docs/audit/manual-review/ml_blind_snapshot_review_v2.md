# Blind snapshot review worksheet (`ml-blind-snapshot-review-v2`)

## Purpose

Second reviewer-blind snapshot worksheet for cleaner manual labels after weak blind transfer diagnostics. The CSV hides ranking scores, ranks, family score JSON, learned logits, and model predictions; those fields live only in the sidecar JSON.

## Provenance

- **worksheet_version:** `ml-blind-snapshot-review-v2`
- **review_pool_variant:** `ml_blind_snapshot_audit`
- **sample_seed:** `20260512`
- **row_id formula:** `sha256(worksheet_version|sample_seed|paper_id)`
- **label_dataset:** `docs/audit/ml-label-dataset-v4.json`
- **label_dataset_sha256:** `88a3067b48f52b6a99295c51e75da54dd03b2b84bd43b9edc674755a28f92288`
- **ranking_run_id:** `rank-ee2ba6c816`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **csv_output:** `docs/audit/manual-review/ml_blind_snapshot_review_v2.csv`
- **context_sidecar_output:** `docs/audit/manual-review/ml_blind_snapshot_review_v2_context.json`
- **markdown_output:** `docs/audit/manual-review/ml_blind_snapshot_review_v2.md`

## Reviewer Blindness

The reviewer CSV excludes `ranking_run_id`, internal Postgres IDs, `final_score`, score components, family score/rank JSON, learned logits, and model predictions. Selection strata may use ranking DB fields off-worksheet for sampling diversity only; the worksheet itself is not sorted by `final_score` or any ranking metric.

## Sidecar Schema

The JSON sidecar is keyed by `row_id` for merge with the CSV and contains `internal_work_id`, `ranking_run_id`, family score/rank JSON, and persisted `paper_scores` features.

## Sample Summary

- **requested rows:** `60`
- **achieved rows:** `60`
- **eligible unlabeled pool size:** `67`
- **excluded as already fully labeled:** `150`

## Row Counts By Sample Reason

| sample_reason | rows |
|---|---:|
| `cluster_stratified_seeded` | 36 |
| `year_band_seeded` | 1 |
| `citation_band_seeded` | 2 |
| `weak_family_context_seeded` | 4 |
| `fallback_seeded_fill` | 17 |

## Cluster Coverage

| cluster_id | rows |
|---|---:|
| `c000` | 15 |
| `c003` | 20 |
| `c005` | 5 |
| `c006` | 3 |
| `c007` | 5 |
| `c010` | 10 |
| `c011` | 2 |

## Year Bands

| year_band | rows |
|---|---:|
| `year_ge_2025` | 60 |

## Citation Bands

| citation_band | rows |
|---|---:|
| `cite_0` | 58 |
| `cite_1_9` | 2 |

## Caveats

- This worksheet is for reviewer-blind manual labeling, not validation.
- The reviewer CSV intentionally hides ranking scores, family ranks, learned logits, and model predictions.
- Selection strata may use ranking DB fields off-worksheet for sampling diversity only; the sidecar preserves that provenance.
- Rows are audit-only until a deliberate train/dev/test policy exists.
- No model is trained and no ranking is run by this worksheet command.
