# Blind snapshot review worksheet (`ml-blind-snapshot-review-v1`)

## Purpose

Deterministic, **non-rank-driven** sample of candidate works from the corpus snapshot for offline manual labeling. 
Rows are selected by cluster, year, citation, and weak-context strata using a seeded RNG; **not** by `final_score` ordering or top-k ranking heads.

## Provenance

- **worksheet_version:** `ml-blind-snapshot-review-v1`
- **sample_seed:** `20260430`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **ranking_run_id_context:** `rank-ee2ba6c816`
- **label_dataset:** `C:/dev/Cursor Projects/Research-Radar/docs/audit/ml-label-dataset-v3.json`
- **csv_output:** `../../docs/audit/manual-review/ml_blind_snapshot_review_v1.csv`
- **markdown_output:** `../../docs/audit/manual-review/ml_blind_snapshot_review_v1.md`
- **generated_at:** `2026-04-30T06:02:57Z`

## Command

```
python -m pipeline.cli ml-blind-snapshot-review-worksheet \
  --label-dataset C:/dev/Cursor Projects/Research-Radar/docs/audit/ml-label-dataset-v3.json \
  --corpus-snapshot-version source-snapshot-v2-candidate-plan-20260428 \
  --embedding-version v2-title-abstract-1536-cleantext-r1 \
  --cluster-version kmeans-l2-v2-cleantext-r1-k12 \
  --ranking-run-id rank-ee2ba6c816 \
  --rows 60 \
  --seed 20260430 \
  --output ../../docs/audit/manual-review/ml_blind_snapshot_review_v1.csv \
  --markdown-output ../../docs/audit/manual-review/ml_blind_snapshot_review_v1.md
```

## Sample summary

- **Requested rows:** 60
- **Achieved rows:** 60 (target after exclusions: 60)
- **Eligible unlabeled pool size:** 127
- **Excluded as already fully labeled:** 90

## Row counts by sample_reason

| sample_reason | rows |
| --- | ---: |
| `cluster_stratified_seeded` | 46 |
| `year_band_seeded` | 1 |
| `citation_band_seeded` | 2 |
| `weak_family_context_seeded` | 4 |
| `fallback_seeded_fill` | 7 |

## Row counts by cluster_id

| cluster_id | rows |
| --- | ---: |
| `c000` | 11 |
| `c001` | 2 |
| `c003` | 7 |
| `c004` | 4 |
| `c005` | 5 |
| `c006` | 7 |
| `c007` | 5 |
| `c008` | 5 |
| `c009` | 4 |
| `c010` | 6 |
| `c011` | 4 |

## Row counts by year band

| year_band | rows |
| --- | ---: |
| `year_2023_2024` | 2 |
| `year_ge_2025` | 58 |

## Row counts by citation band

| citation_band | rows |
| --- | ---: |
| `cite_0` | 53 |
| `cite_1_9` | 7 |

## Caveats

- This worksheet is not validation of ranking quality.
- Rows are sampled for offline audit labeling, not training or production use.
- The split remains audit_only until a deliberate train/dev/test policy exists.
- Ranking context fields are provided for provenance and reviewer context; they must not be treated as labels.
