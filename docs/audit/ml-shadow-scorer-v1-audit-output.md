# ML Shadow Scorer v1 Audit Output (ml-shadow-scorer-v1-audit-output)

## Executive Summary

This isolated audit file executes the disabled `ml-shadow-scorer-v1` formula over the committed fresh validation candidate rows. It writes JSON/Markdown audit output only; it does not enable online shadowing or production behavior.

- Status: succeeded
- Candidate pool size: 358
- Output rows: 358
- Learned probability coverage: 358 / 358
- Shadow execution enabled: False
- Production default changed: False
- Recommended next stage: `draft_ml_shadow_scorer_v1_audit_output_gates`

## Source Contract

- Scorer ID: `ml-shadow-scorer-v1`
- Formula ID: `hybrid_rank_mean_50_50`
- Ranking run / family: `rank-9f4b2a2084` / `emerging`
- Candidate pool SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`
- The readiness isolation contract is satisfied for the offline audit-file path only.

## Verification

- Output matches validation replay: True
- Max absolute score delta: 0
- Max absolute rank percentile delta: 0
- Mismatched work count: 0
- Replay tolerance: 1e-12

## Score Distribution

- Shadow score min / p25 / median / p75 / max: 0.00980392156863 / 0.306022408964 / 0.516806722689 / 0.710784313725 / 0.994397759104
- Shadow score mean: 0.5

## Top-K Preview

| Shadow rank | Work | Score | Final rank pct | Learned rank pct | Heuristic rank |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | `W4408772031` | 0.994397759104 | 0.991596638655 | 0.997198879552 | 4 |
| 2 | `W4402645135` | 0.973389355742 | 0.977591036415 | 0.96918767507 | 9 |
| 3 | `W4410091503` | 0.966386554622 | 0.966386554622 | 0.966386554622 | 13 |
| 4 | `W4407236737` | 0.942577030812 | 0.957983193277 | 0.927170868347 | 16 |
| 5 | `W7119099299` | 0.942577030812 | 0.901960784314 | 0.983193277311 | 36 |
| 6 | `W4414799845` | 0.936974789916 | 0.980392156863 | 0.893557422969 | 8 |
| 7 | `W4412780451` | 0.934173669468 | 0.949579831933 | 0.918767507003 | 19 |
| 8 | `W4409215215` | 0.927170868347 | 1 | 0.854341736695 | 1 |
| 9 | `W4416267785` | 0.918767507003 | 0.859943977591 | 0.977591036415 | 51 |
| 10 | `W7116976483` | 0.917366946779 | 0.963585434174 | 0.871148459384 | 14 |
| 11 | `W4409215261` | 0.910364145658 | 0.994397759104 | 0.826330532213 | 3 |
| 12 | `W4409967775` | 0.90756302521 | 0.974789915966 | 0.840336134454 | 10 |
| 13 | `W7134122976` | 0.906162464986 | 0.882352941176 | 0.929971988796 | 43 |
| 14 | `W4411141958` | 0.90056022409 | 0.87675070028 | 0.924369747899 | 45 |
| 15 | `W4412900768` | 0.886554621849 | 0.971988795518 | 0.801120448179 | 11 |
| 16 | `W4414199528` | 0.880952380952 | 0.988795518207 | 0.773109243697 | 5 |
| 17 | `W4415947443` | 0.872549019608 | 0.935574229692 | 0.809523809524 | 24 |
| 18 | `W4412518308` | 0.862745098039 | 0.887955182073 | 0.837535014006 | 41 |
| 19 | `W4411489043` | 0.858543417367 | 0.918767507003 | 0.798319327731 | 30 |
| 20 | `W7128595024` | 0.857142857143 | 0.831932773109 | 0.882352941176 | 61 |

## Top-K Overlap

| k | Overlap | Jaccard |
| ---: | ---: | ---: |
| 5 | 1 | 0.111111111111 |
| 10 | 4 | 0.25 |
| 20 | 12 | 0.428571428571 |

## Rank Displacement

- Count: 358
- Mean absolute displacement: 45.9720670391
- Median absolute displacement: 37
- p90 absolute displacement: 97.3
- Max absolute displacement: 148

## Observability

- Component coverage: {'final_score_count': 358, 'audit_embedding_probability_work_count': 358, 'shadow_score_count': 358}
- Missing learned probability count: 0
- Family counts: {'emerging': 358}
- Shadow output completeness: {'output_row_count': 358, 'rows_with_shadow_rank': 358, 'rows_with_label_not_used_flag': 358, 'complete': True}
- Error and latency counters are marked not applicable for this offline audit file.

## Blockers

- `missing_ml_shadow_scorer_v1_audit_output_artifact`: False
- `missing_ml_shadow_scorer_v1_spec`: False
- `missing_ml_shadow_scorer_v1_implementation`: False
- `missing_shadow_execution_readiness_gates`: False
- `missing_shadow_output_isolation_check`: False
- `missing_ml_shadow_scorer_v1_audit_output_gates`: True
- `shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False
- `production_default_changed`: False
- `api_web_changed`: False
- `missing_online_shadow_execution_policy`: True
- `missing_shadow_runtime_isolation_verification`: True
- `missing_production_readiness_authorization`: True

## Not Online Shadow / Not Production

- `shadow_execution_enabled` remains false.
- This artifact does not authorize API/web integration, online shadow beside production, user-visible ranking changes, or production default.

## Caveats

- Offline audit JSON only.
- Does not authorize online shadow, API/web, user-visible ranking, or production default.
