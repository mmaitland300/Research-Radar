# ML label readiness matrix

Read-only summary of **manual label coverage** and **offline baseline readiness** by `ranking_run_id`, `family`, and derived target. Uses `ml-label-dataset` plus **`ranking_runs` / `paper_scores`** (read-only). No model training.

## Provenance

- **label_dataset_path:** `C:/dev/Cursor Projects/Research-Radar/docs/audit/ml-label-dataset-v4.json`
- **label_dataset_version:** `ml-label-dataset-v4`
- **label_dataset_sha256:** `96863255e17ad8e7948d6cc1ac53c81b16da68752bda542ebbab0740b6c72dbb`
- **duplicate_row_id_skipped (global):** 0
- **generated_at:** `2026-04-30T06:33:11Z`

## Caveats

- This is not validation.
- Blind snapshot labels reduce but do not eliminate selection bias.
- All rows remain audit_only.
- No production ranking change is supported.

## Recommendation

Run `ml-offline-baseline-eval` for each succeeded `ranking_run_id` that appears under `run_ml_offline_baseline_eval_for` once you care about score-aligned metrics for those slices. For groups without both classes or below diagnostic counts, prioritize **targeted worksheets** (explicit negatives / contrastive rows) before expecting stable AUC or tiny baselines.

- **Runs with both classes (candidates for `ml-offline-baseline-eval`):** `rank-ee2ba6c816`

- **Groups needing richer / contrastive labeling (heuristic):** 17

## Run snapshots (DB)

See JSON `run_snapshots` for `ranking_run_exists`, `ranking_run_succeeded`, `ranking_run_status`, and `paper_scores_row_count` per `ranking_run_id`.

## Groups (detail)

See JSON `groups` for per (`ranking_run_id`, `family`, `target`) counts, join coverage, conflicts, readiness flags, and `review_pool_variant_counts`.

## Source-slice summary

See JSON `source_slice_summary` for per-slice diagnostics (`positive_count`, `negative_count`, `null_count`, `has_both_classes`, `enough_for_diagnostic_auc`, `enough_for_tiny_baseline`) plus `review_pool_variant_counts`.
