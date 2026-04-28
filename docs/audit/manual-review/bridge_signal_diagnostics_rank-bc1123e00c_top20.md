# Bridge signal diagnostics

Diagnostic only: this report does **not** validate bridge ranking and does **not** prove ML ranking is better.

## Provenance

- **ranking_run_id:** `rank-bc1123e00c`
- **ranking_version:** `bridge-v2-nm1-w005-corpusv2-r3-k12-elig-top50-cross040-20260428`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **cluster_version:** `kmeans-l2-v2-cleantext-r1-k12`
- **k:** `20`

## Key blocker summary

- **Full bridge top-k equals eligible-only top-k:** `False` (eligibility filter not selective at head when true).
- **Full bridge vs emerging Jaccard (top-k work_id sets):** `0.73913`
- **Eligible-only bridge vs emerging Jaccard (top-k work_id sets):** `0.212121`
- **Emerging overlap delta (full - eligible):** `0.527009`
- **High emerging overlap (Jaccard ≥ 0.50):** `True`
- **Low bridge_score variance in top-k:** `False`
- **Signal details missing or sparse:** `False`

## Bridge eligibility coverage (all bridge-family rows)

- **bridge_family_row_count:** `217`
- **bridge_eligible_true_count:** `93`
- **bridge_eligible_false_count:** `124`
- **bridge_eligible_null_count:** `0`
- **bridge_score_nonnull_count:** `217`
- **bridge_score_null_count:** `0`
- **bridge_signal_json_present_count:** `217`
- **bridge_signal_json_missing_count:** `0`

## Score distribution

### All bridge rows

- **min / max / mean / median:** `0.0` / `0.996747` / `0.825239659` / `0.833216`
- **unique_bridge_score_count:** `217`
- **null_bridge_score_count:** `0`

### Bridge top-k

- **min / max / mean / median:** `0.648657` / `0.986613` / `0.84597975` / `0.866386`
- **unique_bridge_score_count:** `20`
- **null_bridge_score_count:** `0`

## Overlap detail (top-k)

- **Full bridge overlap count (bridge ∩ emerging):** `17`
- **Full bridge vs emerging Jaccard:** `0.73913`
- **Eligible-only bridge overlap count (bridge_eligible=true ∩ emerging):** `7`
- **Eligible-only bridge vs emerging Jaccard:** `0.212121`
- **Full vs eligible bridge overlap count:** `10`
- **Full vs eligible bridge Jaccard:** `0.333333`
- **Emerging overlap delta (full - eligible):** `0.527009`
- **Bridge-only count:** `3`
- **Emerging-only count:** `3`

## Compact bridge top-k (scores + overlap flags)

| rank | work_id | in_em | in_uc | eligible | signal (summary) | final | sem | cite | topic | bridge | div |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 136 | True | True | False | `neighbor_mix_v1; elig=False; mix=0.3333333333333333; n=15` | 0.861406 | None | 1.0 | 1.0 | 0.889527 | 0.666667 |
| 2 | 21 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.4666666666666667; n=15` | 0.812312 | None | 0.454545 | 1.0 | 0.876733 | 0.0 |
| 3 | 19 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.7333333333333333; n=15` | 0.802589 | None | 0.454545 | 1.0 | 0.672552 | 0.0 |
| 4 | 104 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.8666666666666667; n=15` | 0.752191 | None | 0.272727 | 1.0 | 0.886925 | 0.0 |
| 5 | 15 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.7333333333333333; n=15` | 0.746458 | None | 0.272727 | 1.0 | 0.766523 | 0.0 |
| 6 | 20 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.3333333333333333; n=15` | 0.746291 | None | 0.272727 | 1.0 | 0.763029 | 0.0 |
| 7 | 14 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.8; n=15` | 0.722518 | None | 0.181818 | 1.0 | 0.900149 | 0.0 |
| 8 | 11 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.3333333333333333; n=15` | 0.717803 | None | 0.181818 | 1.0 | 0.801144 | 0.0 |
| 9 | 16 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.8666666666666667; n=15` | 0.710542 | None | 0.181818 | 1.0 | 0.648657 | 0.0 |
| 10 | 125 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.6; n=15` | 0.696332 | None | 0.090909 | 1.0 | 0.986613 | 0.0 |
| 11 | 131 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.8666666666666667; n=15` | 0.695414 | None | 0.090909 | 1.0 | 0.96734 | 0.0 |
| 12 | 143 | False | False | True | `neighbor_mix_v1; elig=True; mix=0.8666666666666667; n=15` | 0.695055 | None | 0.090909 | 1.0 | 0.959798 | 0.0 |
| 13 | 10 | True | False | True | `neighbor_mix_v1; elig=True; mix=1.0; n=15` | 0.694212 | None | 0.090909 | 1.0 | 0.942087 | 0.0 |
| 14 | 126 | False | False | True | `neighbor_mix_v1; elig=True; mix=0.8; n=15` | 0.690114 | None | 0.090909 | 1.0 | 0.856039 | 0.0 |
| 15 | 138 | True | False | True | `neighbor_mix_v1; elig=True; mix=0.6666666666666666; n=15` | 0.6896 | None | 0.272727 | 1.0 | 0.972509 | 0.333333 |
| 16 | 9 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.6; n=15` | 0.687273 | None | 0.090909 | 1.0 | 0.796375 | 0.0 |
| 17 | 30 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.9333333333333333; n=15` | 0.686396 | None | 0.090909 | 1.0 | 0.777947 | 0.0 |
| 18 | 12 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.6; n=15` | 0.680762 | None | 0.090909 | 1.0 | 0.659645 | 0.0 |
| 19 | 137 | True | False | False | `neighbor_mix_v1; elig=False; mix=0.13333333333333333; n=15` | 0.676574 | None | 0.454545 | 1.0 | 0.826236 | 0.666667 |
| 20 | 4 | False | True | True | `neighbor_mix_v1; elig=True; mix=0.8; n=15` | 0.665227 | None | 0.0 | 1.0 | 0.969767 | 0.0 |

Titles omitted from the table width; see JSON `bridge_top_k_rows` for `paper_id` and full `title`. Raw `bridge_signal_json` is not inlined in Markdown.

## Cluster / signal diagnostics (bridge top-k)

- **Distinct anchor clusters:** `9`
- **Distinct neighbor clusters (from neighbor lists × clusters table):** `12`
- **Rows missing cluster/signal resolution:** `0`

### Top cluster pairs (anchor → neighbor)

- `c010` → `c010`: **26**
- `c000` → `c000`: **21**
- `c003` → `c003`: **21**
- `c006` → `c003`: **15**
- `c001` → `c008`: **11**
- `c009` → `c010`: **11**
- `c001` → `c001`: **10**
- `c001` → `c010`: **9**
- `c003` → `c008`: **9**
- `c010` → `c003`: **6**
- `c000` → `c010`: **6**
- `c003` → `c010`: **6**
- `c008` → `c008`: **6**
- `c011` → `c011`: **6**
- `c000` → `c007`: **5**

## Diagnosis flags (hints only)

- **eligibility_filter_not_selective_at_head:** `False`
- **bridge_score_has_low_variance:** `False`
- **bridge_head_emerging_overlap_high:** `True`
- **bridge_signal_details_missing_or_sparse:** `False`
- **eligible_head_differs_from_full:** `True`
- **eligible_head_less_emerging_like_than_full:** `True`
- **eligible_distinctness_improves_by_threshold (delta >= 0.10):** `True`

## Suggested next step

- `increase_bridge_score_weight_only_after_new_distinctness_run`

## Limitations

- Operator-facing diagnostic only; not statistical validation.
- Does not change ranking weights or persisted signals.
