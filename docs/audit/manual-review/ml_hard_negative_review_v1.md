# Hard-negative review worksheet (`ml-hard-negative-review-v1`)

## Purpose

Reviewer-blind worksheet for deliberate negative and borderline relevance-boundary examples. This is a label-quality audit artifact, not model training, ranking, validation, or production readiness.

## Provenance

- **worksheet_version:** `ml-hard-negative-review-v1`
- **review_pool_variant:** `ml_hard_negative_audit`
- **sample_seed:** `20260513`
- **row_id formula:** `sha256(worksheet_version|sample_seed|paper_id)`
- **label_dataset:** `docs/audit/ml-label-dataset-v5.json`
- **label_dataset_sha256:** `0dde7a62d4e7d628aa7626f5501c0982603188e1542bc381ec054e615b1ff6d7`
- **conflict_policy:** `docs/audit/ml-label-conflict-policy.md`
- **conflict_policy_sha256:** `d0591de1a2bd9ab75c64f2318e9a5ea0b7acd94902e083931049893416d47841`
- **ranking_run_id:** `rank-ee2ba6c816`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **csv_output:** `docs/audit/manual-review/ml_hard_negative_review_v1.csv`
- **context_sidecar_output:** `docs/audit/manual-review/ml_hard_negative_review_v1_context.json`
- **markdown_output:** `docs/audit/manual-review/ml_hard_negative_review_v1.md`

## Reviewer CSV Policy

The reviewer CSV excludes `ranking_run_id`, `internal_work_id`, score/rank fields, family score/rank JSON, learned logits, model predictions, and the global `corpus_snapshot_version`, `embedding_version`, and `cluster_version` strings. Those fields are preserved in the sidecar and this Markdown only.

## Exclusion Rule

A work is excluded if any row in the v5 label dataset for the same OpenAlex work already has at least one non-empty manual label field among `relevance_label`, `novelty_label`, or `bridge_like_label`.

## Sample Summary

- **requested rows:** `60`
- **achieved rows:** `7`
- **shortfall:** `53`
- **raw candidate pool size:** `217`
- **eligible after any-label exclusion:** `7`
- **excluded by any-label rule:** `210`
- **credible hard-negative / near-miss candidates:** `7`
- **pool supported requested hard-negative intent:** `false`
- **selection note:** Shortfall: conservative v5 exclusion left fewer credible hard-negative / near-miss candidates than requested.

## Shortfall

The worksheet intentionally emits 7 rows instead of padding to 60. The conservative v5 exclusion rule leaves too few credible hard-negative / near-miss candidates in the current curated snapshot.

## Row Counts By Sample Reason

| sample_reason | rows |
|---|---:|
| `weak_music_audio_context` | 1 |
| `education_or_health_surface_match` | 1 |
| `industrial_or_bioacoustic_surface_match` | 1 |
| `low_family_score_near_miss` | 2 |
| `lexical_music_surface_match` | 2 |

## Cluster Coverage

| cluster_id | rows |
|---|---:|
| `c000` | 1 |
| `c003` | 1 |
| `c010` | 5 |

## Year Bands

| year_band | rows |
|---|---:|
| `year_ge_2025` | 7 |

## Citation Bands

| citation_band | rows |
|---|---:|
| `cite_0` | 6 |
| `cite_1_9` | 1 |

## Future Ingest Note

When labeled, a later dataset ingest should merge the sidecar by `row_id` and keep `review_pool_variant=ml_hard_negative_audit` distinct unless an experiment explicitly pools it with blind snapshot rows.

## Caveats

- This worksheet is not validation.
- Rows are for offline hard-negative / near-miss manual labeling only.
- The reviewer CSV intentionally hides ranking scores, ranks, family-score JSON, model predictions, and global snapshot/version strings.
- No model is trained, no ranking is run, and no production ranking change is supported.
