# Manual label dataset (ml-label-dataset-v1)

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
| `good_or_acceptable` | `true` if `relevance_label` ∈ {good, acceptable}; `false` if ∈ {miss, irrelevant}; else `null` |
| `surprising_or_useful` | `true` if `novelty_label` ∈ {surprising, useful}; `false` if ∈ {obvious, not_useful, neither}; else `null` |
| `bridge_like_yes_or_partial` | `true` if `bridge_like_label` ∈ {yes, partial}; `false` if `no`; `null` if missing, empty, `not_applicable`, or unknown token |

## Known biases

- **Single reviewer** per audit pass unless a source file states otherwise.
- **Top-k / worksheet selection**: labels exist for papers that reached audit worksheets, not a random sample of the corpus.
- **Family-specific contexts** (bridge, emerging, undercited, experiment deltas) are not interchangeable without careful experimental design.

## Duplicate and conflicting labels

- **Duplicate `paper_id` count** (papers with more than one retained row): 39
- **Conflicting label groups** (same `paper_id`, same field, multiple distinct non-empty values): 55

Duplicate appearances are **preserved as separate rows** when the same paper was reviewed in different worksheet contexts.

## Skipped blank rows

Total data rows skipped for blank label scaffold: **115** (per-source counts are in JSON metadata `skipped_blank_row_counts_by_source`).

## Split field (`audit_only`)

Every row has `split: "audit_only"` to mark that these observations come from **audit worksheets**, not from a deliberately constructed ML split. Future experiments must assign splits explicitly to avoid leakage.

## Using this in future offline experiments

- Join rows to frozen ranking outputs or corpus snapshots using `ranking_run_id`, `ranking_version`, `corpus_snapshot_version`, `paper_id` / `work_id`, and ranks as appropriate.
- Treat duplicate `paper_id` entries as **separate contexts** unless you define an aggregation policy.
- Use derived targets only when the corresponding raw label is in the documented closed sets.

## Caveats (verbatim)

> This dataset is not validation of bridge ranking quality.

> Labels are single-reviewer offline audit material unless a source explicitly says otherwise.

> Rows come from ranking outputs and review worksheets, so the dataset may contain ranking-selection bias.

> The split field defaults to audit_only; train/dev/test splits must be created deliberately in a later experiment.


## JSON artifact

Machine-readable export: `docs/audit/ml-label-dataset-v1.json` (regenerate via `python -m pipeline.cli ml-label-dataset`).
