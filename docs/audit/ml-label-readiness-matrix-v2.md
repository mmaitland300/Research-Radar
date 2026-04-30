# ML label readiness matrix

Read-only summary of **manual label coverage** and **offline baseline readiness** by `ranking_run_id`, `family`, and derived target. Uses `ml-label-dataset` plus **`ranking_runs` / `paper_scores`** (read-only). No model training.

## Provenance

- **label_dataset_path:** `C:/dev/Cursor Projects/Research-Radar/docs/audit/ml-label-dataset-v2.json`
- **label_dataset_version:** `ml-label-dataset-v2`
- **label_dataset_sha256:** `c5b908973cd033ab363c6673a7b0955fe176406742449aac09fd800855829722`
- **duplicate_row_id_skipped (global):** 0
- **generated_at:** `2026-04-30T03:06:19Z`

## Caveats

- Labels are single-reviewer manual audit material unless a source states otherwise.
- Rows are biased by ranking outputs and worksheet selection (ranking-selection bias).
- This matrix is not validation of ranking quality or of any future model.
- This artifact does not create or imply train/dev/test splits.

## Recommendation

Run `ml-offline-baseline-eval` for each succeeded `ranking_run_id` that appears under `run_ml_offline_baseline_eval_for` once you care about score-aligned metrics for those slices. For groups without both classes—or below diagnostic counts—prioritize **targeted worksheets** (explicit negatives / contrastive rows) before expecting stable AUC or tiny baselines.

- **Runs with both classes (candidates for `ml-offline-baseline-eval`):** `rank-ee2ba6c816`

- **Groups needing richer / contrastive labeling (heuristic):** 19

## Run snapshots (DB)

See JSON `run_snapshots` for `ranking_run_exists`, `ranking_run_succeeded`, `ranking_run_status`, and `paper_scores_row_count` per `ranking_run_id`.

## Groups (detail)

See JSON `groups` for per (`ranking_run_id`, `family`, `target`) counts, join coverage, conflicts, and readiness flags.
