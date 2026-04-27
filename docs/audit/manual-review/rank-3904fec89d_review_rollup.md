# Recommendation review rollup

## Provenance

- **ranking_run_id:** `rank-3904fec89d`
- **ranking_version:** `bridge-v2-nm1-zero-r3-k6-20260424`
- **corpus_snapshot_version:** `source-snapshot-20260425-044015`
- **embedding_version:** `v1-title-abstract-1536-cleantext-r3`
- **cluster_version:** `kmeans-l2-v0-cleantext-r3-k6`

## Family metrics

| Family | P@k good-only | P@k good/acceptable | Bridge-like yes/partial | Surprising/useful |
| --- | --- | --- | --- | --- |
| bridge | 0.900 | 1.000 | 1.000 | 1.000 |
| emerging | 1.000 | 1.000 | null | 1.000 |
| undercited | 0.700 | 1.000 | null | 1.000 |

## Interpretation

- Best good-only family: **emerging**
- Weakest good-only family: **undercited**
- Ready for distinctness analysis: **True**
- Ready for weight experiment: **True**

## Limitations

- Single-reviewer labels can be noisy; treat as directional evidence.
- Small curated corpus can saturate relevance and novelty metrics.
- This rollup is not run-to-run validation and does not prove weight effectiveness.

## Suggested next step

- candidate signal only (not validation): join this rollup with bridge distinctness and top-k overlap before any small weight experiment
