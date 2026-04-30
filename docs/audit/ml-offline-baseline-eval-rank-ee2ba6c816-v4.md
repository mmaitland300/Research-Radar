# Offline label baseline evaluation

Read-only join of **ml-label-dataset** rows to persisted **`paper_scores`** for one explicit `ranking_run_id`. Metrics are **label-aware** (manual `good_or_acceptable`, `surprising_or_useful`, `bridge_like_yes_or_partial` only) and stratified by **recommendation family**. No model training and no database writes.

## Provenance

- **ranking_run_id:** `rank-ee2ba6c816`
- **ranking_version:** `bridge-v2-nm1-zero-corpusv2-r2-k12-elig-top50-cross040-20260428`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **label_dataset_path:** `C:/dev/Cursor Projects/Research-Radar/docs/audit/ml-label-dataset-v4.json`
- **label_dataset_version:** `ml-label-dataset-v4`
- **label_dataset_sha256:** `88a3067b48f52b6a99295c51e75da54dd03b2b84bd43b9edc674755a28f92288`
- **generated_at:** `2026-04-30T06:47:37Z`

## Join summary

- **Label rows (audit_only, run match, after row_id dedupe):** 190
- **Duplicate row_id rows skipped:** 0
- **Joined to paper_scores:** 130
- **Missing from ranking (no score row for family/work):** 60

## Interpretation (readout, not conclusions)

- This artifact is a **diagnostic offline label eval, not validation** of production ranking quality.
- Labels here are **sparse single-reviewer** audit material tied to specific worksheets and runs; do not treat them as ground truth for the full corpus.
- The ranking compared is a **heuristic baseline only**; there is **no learned model** in this pipeline step.
- **Next step** toward ML experiments would be a **simple feature baseline** (e.g. linear model on persisted scores) **only if** label coverage grows enough - especially **negatives** - for stable offline metrics.

### Label coverage vs. simple learned baseline

At least one family x target pair has **both** positive and negative manual labels among rows that joined to `paper_scores`, so rank-based AUC / pairwise accuracy and precision@k can be non-null where row counts allow. That still does **not** imply enough data for a stable learned baseline.

## Caveats

- This is not validation.
- Blind snapshot labels reduce but do not eliminate selection bias.
- All rows remain audit_only.
- No production ranking change is supported.

- **Duplicate labeled observations** for the same `paper_id` remain separate rows when `row_id` differs; metrics treat each joined row independently.

## Metrics (by family and target)

See JSON `metrics.by_family` for `good_or_acceptable`, `surprising_or_useful`, `bridge_like_yes_or_partial`, including precision@k (k=5,10,20 when at least k matched labeled rows exist), ROC AUC (Mann-Whitney rank), and pairwise accuracy.

### Families present

`bridge`, `emerging`, `undercited`
