# Offline label baseline evaluation

Read-only join of **ml-label-dataset** rows to persisted **`paper_scores`** for one explicit `ranking_run_id`. Metrics are **label-aware** (manual `good_or_acceptable`, `surprising_or_useful`, `bridge_like_yes_or_partial` only) and stratified by **recommendation family**. No model training and no database writes.

## Provenance

- **ranking_run_id:** `rank-60910a47b4`
- **ranking_version:** `bridge-v2-nm1-zero-corpusv2-r5-k12-elig-exclude-persistent-v1-20260429`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **label_dataset_path:** `C:/dev/Cursor Projects/Research-Radar/docs/audit/ml-label-dataset-v1.json`
- **label_dataset_version:** `ml-label-dataset-v1`
- **label_dataset_sha256:** `f404f995f2da9607a1448d51dd51c1c130dcb0b3a8945501a3a6d3f133f3e607`
- **generated_at:** `2026-04-29T21:23:39Z`

## Join summary

- **Label rows (audit_only, run match, after row_id dedupe):** 1
- **Duplicate row_id rows skipped:** 0
- **Joined to paper_scores:** 1
- **Missing from ranking (no score row for family/work):** 0

## Interpretation (readout, not conclusions)

- This artifact is a **diagnostic offline label eval, not validation** of production ranking quality.
- Labels here are **sparse single-reviewer** audit material tied to specific worksheets and runs; do not treat them as ground truth for the full corpus.
- The ranking compared is a **heuristic baseline only**; there is **no learned model** in this pipeline step.
- **Next step** toward ML experiments would be a **simple feature baseline** (e.g. linear model on persisted scores) **only if** label coverage grows enough—especially **negatives**—for stable offline metrics.

### Label coverage vs. simple learned baseline

For this `ranking_run_id`, **no** family×target slice has a negative (false) class among joined labeled rows—counts are positive-only or empty. **Discrimination metrics (AUC, pairwise accuracy, meaningful P@k) cannot apply.** A simple feature baseline or learned ranker would need **more labeled negatives** (and usually more rows overall) before offline training experiments are informative.

## Caveats

- Offline audit baseline only; not validation of production ranking quality.
- Labels are single-reviewer manual audit material unless a source states otherwise.
- Rows are biased by ranking outputs and worksheet selection (ranking-selection bias).
- This evaluation does not create or imply train/dev/test splits.

- **Duplicate labeled observations** for the same `paper_id` remain separate rows when `row_id` differs; metrics treat each joined row independently.

## Metrics (by family and target)

See JSON `metrics.by_family` for `good_or_acceptable`, `surprising_or_useful`, `bridge_like_yes_or_partial`, including precision@k (k=5,10,20 when at least k matched labeled rows exist), ROC AUC (Mann–Whitney rank), and pairwise accuracy.

### Families present

`bridge`, `emerging`, `undercited`
