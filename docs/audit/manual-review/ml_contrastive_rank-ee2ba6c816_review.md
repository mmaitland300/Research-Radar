# ML contrastive offline audit worksheet

## Purpose

This artifact lists manually reviewable candidate rows drawn from a **single persisted ranking run** to expand **contrastive offline audit label coverage** (especially sparse negatives and uncertain rows). It does **not** train or evaluate a learned model by itself.

## Selection strategy

- Exclude papers that already have **all three** manual label columns filled for this run + family in the audit slice of the label dataset.
- Prefer ranks **40-80**, score values **near the family median**, and **weak family-specific signals** (including bridge ineligibility where applicable), then fill remaining slots deterministically.
- Rows with **incomplete** prior labels in the dataset may appear with `sample_reason=label_incomplete`; label columns are left blank for a fresh pass.

## Row counts by family

| family | rows |
| --- | ---: |
| `bridge` | 15 |
| `emerging` | 15 |
| `undercited` | 15 |

## Row counts by sample_reason

| sample_reason | rows |
| --- | ---: |
| `bridge_ineligible` | 7 |
| `lower_rank_window` | 15 |
| `median_borderline` | 9 |
| `weak_family_signal` | 14 |

## Caveats

- This worksheet is not validation of ranking quality.
- Rows are selected from a persisted ranking run to improve contrastive label coverage.
- Labels must be filled manually; missing labels must not be inferred.
- Do not create train/dev/test splits from this worksheet until a later explicit split policy exists.

- Train/dev/test split policy is intentionally deferred; do not derive splits from this worksheet alone.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **ranking_version:** `bridge-v2-nm1-zero-corpusv2-r2-k12-elig-top50-cross040-20260428`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **label_dataset:** `docs/audit/ml-label-dataset-v1.json`
- **generated_at:** `2026-04-30T16:52:01Z`

## Duplicate paper notes

- `W4411141538` appears in more than one family row in this worksheet (same persisted pool edge case).
- `W4415315857` appears in more than one family row in this worksheet (same persisted pool edge case).
- `W4412780451` appears in more than one family row in this worksheet (same persisted pool edge case).
