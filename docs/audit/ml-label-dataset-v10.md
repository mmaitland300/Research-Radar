# Manual label dataset (ml-label-dataset-v10)

## What this dataset is

A versioned export of **explicit manual reviewer labels** taken from Research Radar **offline audit CSV worksheets** under `docs/audit/manual-review/`. Each row is one labeled observation of one paper in a specific ranking or experiment-review context, with file-level provenance (path, SHA-256, spreadsheet row number). It exists so future work can run **offline** ranking or learning-to-rank experiments with measurable labels that were **not invented for ML**.

## What this dataset is not

- It is **not** model training output and **not** an automated relevance oracle.
- It is **not** a substitute for live product metrics.
- It does **not** define train/dev/test partitions (see `split`).

## Label sources

Worksheets are CSV exports produced during manual audit. Only rows with at least one non-empty value among `relevance_label`, `novelty_label`, or `bridge_like_label` are included. Free-text `reviewer_notes` alone does not qualify.

### Source files

- `docs/audit/manual-review/bridge_eligible_rank-bc1123e00c_top50.csv`
- `docs/audit/manual-review/bridge_eligible_rank-ee2ba6c816_top20.csv`
- `docs/audit/manual-review/bridge_objective_delta_rank-60910a47b4_one_row_review.csv`
- `docs/audit/manual-review/bridge_objective_elig_delta_rank-60910a47b4_vs_rank-ee2ba6c816_review.csv`
- `docs/audit/manual-review/bridge_rank-3904fec89d_top20.csv`
- `docs/audit/manual-review/bridge_rank-3904fec89d_top20_labeled.csv`
- `docs/audit/manual-review/bridge_weight_experiment_rank-bc1123e00c_delta_review.csv`
- `docs/audit/manual-review/emerging_rank-3904fec89d_top20.csv`
- `docs/audit/manual-review/emerging_rank-3904fec89d_top20_labeled.csv`
- `docs/audit/manual-review/emerging_rank-ee2ba6c816_top20.csv`
- `docs/audit/manual-review/fresh_hybrid_eval_v1_labeled_2026-05-19.csv`
- `docs/audit/manual-review/fresh_hybrid_positive_topup_v1_labeled_2026-05-20.csv`
- `docs/audit/manual-review/ml_blind_snapshot_review_v1.csv`
- `docs/audit/manual-review/ml_blind_snapshot_review_v2_labeled_2026-05-13.csv`
- `docs/audit/manual-review/ml_contrastive_rank-ee2ba6c816_review.csv`
- `docs/audit/manual-review/ml_emerging_gap_rank-ee2ba6c816_review.csv`
- `docs/audit/manual-review/ml_external_near_miss_review_v1_labeled_2026-05-13.csv`
- `docs/audit/manual-review/ml_hard_negative_review_v1_labeled_2026-05-13.csv`
- `docs/audit/manual-review/ml_transfer_gap_review_v1_labeled_2026-05-15.csv`
- `docs/audit/manual-review/undercited_rank-3904fec89d_top20.csv`
- `docs/audit/manual-review/undercited_rank-3904fec89d_top20_labeled.csv`
- `docs/audit/manual-review/undercited_rank-ee2ba6c816_top20.csv`

### Skipped blank worksheets

- `docs/audit/manual-review/bridge_eligible_rank-bc1123e00c_top50.csv`
- `docs/audit/manual-review/bridge_objective_elig_delta_rank-60910a47b4_vs_rank-ee2ba6c816_review.csv`
- `docs/audit/manual-review/bridge_rank-3904fec89d_top20.csv`
- `docs/audit/manual-review/emerging_rank-3904fec89d_top20.csv`
- `docs/audit/manual-review/undercited_rank-3904fec89d_top20.csv`

## Derived targets

These are **deterministic functions** of the three manual label columns only (no inference from scores or titles):

| Column | Rule |
|--------|------|
| `good_or_acceptable` | `true` if `relevance_label` is one of good, acceptable; `false` if one of miss, irrelevant; else `null` |
| `surprising_or_useful` | `true` if `novelty_label` is one of surprising, useful; `false` if one of obvious, not_useful, neither; else `null` |
| `bridge_like_yes_or_partial` | `true` if `bridge_like_label` is one of yes, partial; `false` if `no`; `null` if missing, empty, `not_applicable`, or unknown token |

## Known biases

- **Single reviewer** per audit pass unless a source file states otherwise.
- **Top-k / worksheet selection**: labels exist for papers that reached audit worksheets, not a random sample of the corpus.
- **Family-specific contexts** (bridge, emerging, undercited, experiment deltas) are not interchangeable without careful experimental design.
- **Transfer-gap targeted samples** are not representative validation data.

## Family inference (worksheet context)

Some bridge experiment review CSVs (weight delta review, objective delta / eligibility delta / one-row review) do not include a `family` column. For those files only, `family` is set to **`bridge`** from worksheet naming convention so downstream joins can treat rows like other bridge-family audits. This does **not** change any reviewer label columns.

- **Rows with inferred `family`:** 5 (per-source counts: `metadata.inferred_family_by_source`).

## Blind snapshot context fields

Rows from worksheets with `review_pool_variant=ml_blind_snapshot_audit` keep `family=null` (these papers were **not** sampled from a recommendation family's top-k). To support a blind-source family-context diagnostic, these rows additionally preserve worksheet-level context when the worksheet provides it: `worksheet_version`, `sample_seed`, `sample_reason`, `cluster_id`, `topics`, `abstract_preview`, `ranking_context_family_scores_json`, `ranking_context_family_ranks_json`, `openalex_work_id`, and `internal_work_id`. Reviewer-blind v2 rows also keep the full sidecar row in nested `blind_snapshot_context`, keyed by the canonical worksheet `row_id`, so hidden score/rank provenance is not lost when the reviewer CSV intentionally omits it. These context fields are **not labels** and must not be treated as family-selected ranking outputs.

## Hard-negative context fields

Rows from worksheets with `review_pool_variant=ml_hard_negative_audit` remain a distinct audit pool for negative and near-miss relevance-boundary labels. They keep `family=null` unless a worksheet explicitly provides a family column, and preserve sidecar provenance in nested `hard_negative_context`. That context may include hidden score/rank features and selection signals, but derived targets are still computed only from explicit manual labels.

## External near-miss context fields

Rows from worksheets with `review_pool_variant=ml_external_near_miss_audit` remain a distinct audit pool for negative-boundary labels acquired outside the current curated corpus snapshot. They have `split=audit_only`, `family=null` unless a worksheet explicitly provides a family column, and no top-level ranking or corpus snapshot identity because they were not sampled from a persisted ranking run. The nested `external_near_miss_context` preserves OpenAlex query provenance, outside-217 exclusion checks, and reviewer-hidden acquisition metadata. Those context fields are **not labels** and must not be pooled with blind or hard-negative rows unless a later experiment explicitly says so.

## Transfer-gap context fields

Rows from worksheets with `review_pool_variant=ml_transfer_gap_audit` remain a distinct targeted audit pool for transfer-gap and sparse-pool labeling. They have `split=audit_only`, keep `family=null` unless the sidecar explicitly maps a family field, and preserve the full row-id keyed sidecar object under nested `transfer_gap_context`. The sidecar may describe gap priority, target hint, source query, or old evidence pool being addressed; those context fields are **not labels** and do not imply production ranking readiness.

## Fresh hybrid eval context fields

Rows from worksheets with `review_pool_variant=ml_fresh_hybrid_eval_v1` are manual labels for the fresh hybrid confirmatory path. They have `split=audit_only`, preserve ranking-run context from the worksheet, and keep the full row-id keyed sidecar object under nested `fresh_hybrid_context`. The sidecar provides candidate-surface provenance only; it is not label evidence and does not authorize validation, shadow, or production.

## Fresh hybrid positive top-up context fields

Rows from worksheets with `review_pool_variant=ml_fresh_hybrid_positive_topup_v1` are manual positive-threshold top-up labels for the same fresh hybrid surface after v9 materialization showed only the positive work-count floor was short. They have `split=audit_only`, preserve ranking-run context from the worksheet, and keep the full row-id keyed sidecar object under nested `fresh_hybrid_positive_topup_context`. The sidecar is provenance only; the authoritative threshold pass/fail comes from rerunning the fresh-surface materializer.

### Fresh hybrid positive top-up v1 ingest

- **Rows appended:** 22
- **Legacy rows:** copied from v9 unchanged field-for-field, including their existing per-row `dataset_version` values.
- **Source row numbering:** physical CSV line including header; first data row = 2.
- **Raw relevance distribution:** `{'acceptable': 5, 'good': 10, 'miss': 7}`
- **Raw novelty distribution:** `{'obvious': 8, 'useful': 14}`
- **Raw bridge-like distribution:** `{'not_applicable': 14, 'partial': 8}`
- **good_or_acceptable positives / negatives:** 15 / 7
- **Projected positive count:** 54 = 39 + 15
- **Materializer source of truth:** rematerialize with `ml-label-dataset-v10` to confirm final work-level threshold pass/fail.
- **Next step:** rerun `ml-fresh-eval-surface-hybrid-materialize` with `--label-dataset ../../docs/audit/ml-label-dataset-v10.json --expected-label-dataset-version ml-label-dataset-v10`.

### Fresh hybrid v1 ingest

- **Rows appended:** 120
- **Legacy rows:** copied from v8 unchanged field-for-field, including their existing per-row `dataset_version` values.
- **Source row numbering:** physical CSV line including header; first data row = 2.
- **Raw relevance distribution:** `{'acceptable': 12, 'good': 26, 'irrelevant': 48, 'miss': 34}`
- **Raw novelty distribution:** `{'neither': 48, 'not_useful': 1, 'obvious': 17, 'useful': 54}`
- **Raw bridge-like distribution:** `{'not_applicable': 77, 'partial': 43}`
- **good_or_acceptable positives / negatives:** 38 / 82
- **Next step:** after a follow-up accepts `ml-label-dataset-v9`, rerun `ml-fresh-eval-surface-hybrid-materialize` with `--label-dataset ../../docs/audit/ml-label-dataset-v9.json` to measure remaining policy thresholds.

### Transfer-gap v1 ingest

- **Rows appended:** 45
- **Legacy rows:** copied from v7 unchanged field-for-field, including their existing per-row `dataset_version` values.
- **Source row numbering:** physical CSV line including header; first data row = 2.
- **Raw relevance distribution:** `{'acceptable': 1, 'irrelevant': 17, 'miss': 27}`
- **Raw novelty distribution:** `{'not_useful': 17, 'obvious': 14, 'useful': 14}`
- **Raw bridge-like distribution:** `{'no': 18, 'partial': 26, 'yes': 1}`

## Duplicate and conflicting labels

- **Duplicate `paper_id` count** (papers with more than one retained row): 55
- **Conflicting raw label groups** (same `paper_id`, same label field, multiple distinct non-empty values): 83

**Duplicate rows:** the same `paper_id` may appear in multiple worksheets or ranks. Each row remains a **separate labeled observation**; nothing in this export merges or collapses duplicates - use `row_id` and provenance fields when designing offline baselines.

## Derived target conflicts

For each derived boolean target (`good_or_acceptable`, `surprising_or_useful`, `bridge_like_yes_or_partial`), we group by `paper_id` and compare non-null values only. A conflict is recorded when the same paper has **both** `true` and `false` for that target across rows (e.g. `surprising` vs `obvious` both map into `surprising_or_useful` and therefore do **not** count as a conflict on that target).

- **Derived target conflict count:** 11

## Skipped blank rows

Total data rows skipped for blank label scaffold: **115** (per-source counts are in JSON metadata `skipped_blank_row_counts_by_source`).

## Split field (`audit_only`)

Every row has `split: "audit_only"` to mark that these observations come from **audit worksheets**, not from a deliberately constructed ML split. Future experiments must assign splits explicitly to avoid leakage.

## Using this in future offline experiments

- Join rows to frozen ranking outputs or corpus snapshots using `ranking_run_id`, `ranking_version`, `corpus_snapshot_version`, `paper_id` / `work_id`, and ranks as appropriate.
- Treat duplicate `paper_id` entries as **separate contexts** unless you define an aggregation policy.
- Use derived targets only when the corresponding raw label is in the documented closed sets.

## Caveats (verbatim)

> This is not validation.

> Blind snapshot labels reduce but do not eliminate selection bias.

> All rows remain audit_only.

> No production ranking change is supported.

> Fresh hybrid positive top-up labels are single-reviewer audit labels.

> Fresh hybrid positive top-up rows remain audit_only and do not define a train/eval split.

> This dataset versioning step is not validation and does not run materialization, ranking, scoring, training, or embeddings.

> No production or API behavior changes are authorized by this artifact.


## JSON artifact

Machine-readable export: regenerate via `python -m pipeline.cli ml-label-dataset-v10-fresh-positive-topup-ingest --base-dataset docs/audit/ml-label-dataset-v9.json --blank-worksheet docs/audit/manual-review/fresh_hybrid_positive_topup_v1.csv --labeled-worksheet docs/audit/manual-review/fresh_hybrid_positive_topup_v1_labeled_2026-05-20.csv --context-sidecar docs/audit/manual-review/fresh_hybrid_positive_topup_v1_context.json --conflict-policy docs/audit/ml-label-conflict-policy.md --output <path>.json`.
