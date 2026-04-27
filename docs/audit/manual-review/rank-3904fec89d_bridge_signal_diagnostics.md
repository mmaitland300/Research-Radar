# Bridge signal diagnostics

Diagnostic only: this report does **not** validate bridge ranking and does **not** prove ML ranking is better.

## Provenance

- **ranking_run_id:** `rank-3904fec89d`
- **ranking_version:** `bridge-v2-nm1-zero-r3-k6-20260424`
- **corpus_snapshot_version:** `source-snapshot-20260425-044015`
- **embedding_version:** `v1-title-abstract-1536-cleantext-r3`
- **cluster_version:** `kmeans-l2-v0-cleantext-r3-k6`
- **k:** `20`

## Key blocker summary

- **Full bridge top-k equals eligible-only top-k:** `True` (eligibility filter not selective at head when true).
- **Bridge vs emerging Jaccard (top-k work_id sets):** `0.666667`
- **High emerging overlap (Jaccard ≥ 0.50):** `True`
- **Low bridge_score variance in top-k:** `False`
- **Signal details missing or sparse:** `False`

## Bridge eligibility coverage (all bridge-family rows)

- **bridge_family_row_count:** `59`
- **bridge_eligible_true_count:** `59`
- **bridge_eligible_false_count:** `0`
- **bridge_eligible_null_count:** `0`
- **bridge_score_nonnull_count:** `59`
- **bridge_score_null_count:** `0`
- **bridge_signal_json_present_count:** `59`
- **bridge_signal_json_missing_count:** `0`

## Score distribution

### All bridge rows

- **min / max / mean / median:** `0.482677` / `0.982759` / `0.70637078` / `0.705923`
- **unique_bridge_score_count:** `59`
- **null_bridge_score_count:** `0`

### Bridge top-k

- **min / max / mean / median:** `0.498669` / `0.982759` / `0.6908185` / `0.7047829999999999`
- **unique_bridge_score_count:** `20`
- **null_bridge_score_count:** `0`

## Overlap detail (top-k)

- **Overlap count (bridge ∩ emerging):** `16`
- **Bridge-only count:** `4`
- **Emerging-only count:** `4`

## Compact bridge top-k (scores + overlap flags)

| rank | work_id | in_em | in_uc | eligible | signal (summary) | final | sem | cite | topic | bridge | div |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 733 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.5333333333333333; n=15` | 0.540577 | None | 0.963719 | 0.312732 | 0.504015 | 0.0 |
| 2 | 71 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.4666666666666667; n=15` | 0.536609 | None | 1.0 | 0.287091 | 0.498669 | 0.0 |
| 3 | 37 | True | True | True | `neighbor_mix_v1; elig=True; mix=0.6666666666666666; n=15` | 0.412592 | None | 0.782313 | 0.213512 | 0.654061 | 0.0 |
| 4 | 85 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.6666666666666666; n=15` | 0.38308 | None | 0.680272 | 0.223053 | 0.739483 | 0.0 |
| 5 | 89 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.6; n=15` | 0.377085 | None | 0.544218 | 0.287091 | 0.616061 | 0.0 |
| 6 | 5 | False | True | True | `neighbor_mix_v1; elig=True; mix=0.5333333333333333; n=15` | 0.359477 | None | 0.0 | 0.553042 | 0.668711 | 0.0 |
| 7 | 52 | True | True | True | `neighbor_mix_v1; elig=True; mix=0.6; n=15` | 0.346133 | None | 0.408163 | 0.312732 | 0.708855 | 0.0 |
| 8 | 1 | False | True | True | `neighbor_mix_v1; elig=True; mix=0.6666666666666666; n=15` | 0.336609 | None | 0.0 | 0.51786 | 0.751551 | 0.0 |
| 9 | 23 | False | True | True | `neighbor_mix_v1; elig=True; mix=0.6; n=15` | 0.336609 | None | 0.0 | 0.51786 | 0.601351 | 0.0 |
| 10 | 1137 | False | True | True | `neighbor_mix_v1; elig=True; mix=0.4666666666666667; n=15` | 0.336609 | None | 0.0 | 0.51786 | 0.75554 | 0.0 |
| 11 | 55 | True | True | True | `neighbor_mix_v1; elig=True; mix=0.5333333333333333; n=15` | 0.319347 | None | 0.331633 | 0.312732 | 0.503339 | 0.0 |
| 12 | 709 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.5333333333333333; n=15` | 0.31416 | None | 0.364431 | 0.287091 | 0.54726 | 0.0 |
| 13 | 718 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.6666666666666666; n=15` | 0.294602 | None | 0.189504 | 0.351193 | 0.809784 | 0.0 |
| 14 | 19 | True | True | True | `neighbor_mix_v1; elig=True; mix=0.6; n=15` | 0.292562 | None | 0.255102 | 0.312732 | 0.700711 | 0.0 |
| 15 | 16 | True | True | True | `neighbor_mix_v1; elig=True; mix=0.6666666666666666; n=15` | 0.286858 | None | 0.102041 | 0.386375 | 0.830831 | 0.0 |
| 16 | 15 | True | True | True | `neighbor_mix_v1; elig=True; mix=0.7333333333333333; n=15` | 0.281847 | None | 0.153061 | 0.351193 | 0.747436 | 0.0 |
| 17 | 53 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.6; n=15` | 0.266966 | None | 0.229592 | 0.287091 | 0.77308 | 0.0 |
| 18 | 708 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.9333333333333333; n=15` | 0.266334 | None | 0.364431 | 0.213512 | 0.982759 | 0.0 |
| 19 | 73 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.6; n=15` | 0.265181 | None | 0.22449 | 0.287091 | 0.658677 | 0.0 |
| 20 | 91 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.5333333333333333; n=15` | 0.25783 | None | 0.340136 | 0.213512 | 0.764196 | 0.0 |

Titles omitted from the table width; see JSON `bridge_top_k_rows` for `paper_id` and full `title`. Raw `bridge_signal_json` is not inlined in Markdown.

## Cluster / signal diagnostics (bridge top-k)

- **Distinct anchor clusters:** `6`
- **Distinct neighbor clusters (from neighbor lists × clusters table):** `6`
- **Rows missing cluster/signal resolution:** `0`

### Top cluster pairs (anchor → neighbor)

- `c000` → `c000`: **37**
- `c002` → `c002`: **27**
- `c000` → `c001`: **22**
- `c005` → `c005`: **21**
- `c001` → `c001`: **21**
- `c002` → `c001`: **20**
- `c001` → `c002`: **16**
- `c000` → `c002`: **15**
- `c001` → `c005`: **14**
- `c000` → `c004`: **13**
- `c000` → `c005`: **13**
- `c005` → `c001`: **10**
- `c002` → `c000`: **8**
- `c005` → `c000`: **8**
- `c003` → `c003`: **6**

## Diagnosis flags (hints only)

- **eligibility_filter_not_selective_at_head:** `True`
- **bridge_score_has_low_variance:** `False`
- **bridge_head_emerging_overlap_high:** `True`
- **bridge_signal_details_missing_or_sparse:** `False`

## Suggested next step

- `tighten_bridge_eligibility_thresholds`

## Limitations

- Operator-facing diagnostic only; not statistical validation.
- Does not change ranking weights or persisted signals.
