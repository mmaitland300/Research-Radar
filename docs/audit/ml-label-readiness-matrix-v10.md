# ML label readiness matrix

Read-only summary of **manual label coverage** and **offline baseline readiness** by `ranking_run_id`, `family`, and derived target. Uses `ml-label-dataset` plus **`ranking_runs` / `paper_scores`** (read-only). No model training.

## Provenance

- **label_dataset_path:** `docs/audit/ml-label-dataset-v13.json`
- **label_dataset_version:** `ml-label-dataset-v13`
- **label_dataset_sha256:** `530561e9aeae3158c2c9744c2a8f7d13c103ad0857c6dcb2375ac72d7b994a33`
- **duplicate_row_id_skipped (global):** 0
- **generated_at:** `2026-06-02T04:49:52Z`

## Caveats

- This is not validation.
- Blind snapshot labels reduce but do not eliminate selection bias.
- All rows remain audit_only.
- No production ranking change is supported.

## Recommendation

Run `ml-offline-baseline-eval` for each succeeded `ranking_run_id` that appears under `run_ml_offline_baseline_eval_for` once you care about score-aligned metrics for those slices. For groups without both classes or below diagnostic counts, prioritize **targeted worksheets** (explicit negatives / contrastive rows) before expecting stable AUC or tiny baselines. `bridge_recommendable` is for an offline/diagnostic bridge scorer only and is not production readiness.

- **Runs with both classes (candidates for `ml-offline-baseline-eval`):** `rank-83787b91ef`, `rank-9f4b2a2084`, `rank-ee2ba6c816`

- **Groups needing richer / contrastive labeling (heuristic):** 27

## Run snapshots (DB)

See JSON `run_snapshots` for `ranking_run_exists`, `ranking_run_succeeded`, `ranking_run_status`, and `paper_scores_row_count` per `ranking_run_id`.

## Groups (detail)

See JSON `groups` for per (`ranking_run_id`, `family`, `target`) counts, join coverage, conflicts, readiness flags, and `review_pool_variant_counts`.

## Source-slice summary

See JSON `source_slice_summary` for per-slice diagnostics (`positive_count`, `negative_count`, `null_count`, `has_both_classes`, `enough_for_diagnostic_auc`, `enough_for_tiny_baseline`, `trainable_next_step`) plus `review_pool_variant_counts`. `bridge_recommendable` next steps are offline/diagnostic bridge-scorer guidance only, not production readiness.
