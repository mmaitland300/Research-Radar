# ML Shadow Scorer v1 Implementation Audit (ml-shadow-scorer-v1-implementation)

## Executive Summary

This artifact implements the frozen `ml-shadow-scorer-v1` formula and verifies it by exact replay against `validation["candidate_work_scores"]`. Shadow execution remains disabled by default.

- Implementation matches spec: True
- Implementation matches validation replay: True
- Candidate pool size: 358
- Learned probability coverage: 358 / 358
- Shadow execution enabled: False
- Recommended next stage: `draft_ml_shadow_scorer_v1_execution_readiness_gates`

## Formula Implementation

- `score = 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)`
- The implementation ignores any label fields present in replay rows.

## Replay Input Path

- `validation["candidate_work_scores"]` from `ml-hybrid-validation-on-fresh-surface-v1.json`.

## Rank Percentile Policy

- Higher raw score is better.
- Ties use average rank.
- If n == 1, rank_pct = 1.0.
- Otherwise rank_pct = 1.0 - ((average_rank - 1.0) / (n - 1.0)).

## Exact Replay Result

- Candidate work scores count: 358
- Candidate pool SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`
- Max absolute score delta: 0
- Max absolute rank percentile delta: 0
- Mismatched work count: 0
- Replay tolerance: 1e-12

## Score Distribution

- Min / p25 / median / p75 / max: 0.00980392156863 / 0.306022408964 / 0.516806722689 / 0.710784313725 / 0.994397759104
- Mean: 0.5

## Top-K Preview

| Rank | Work | Score | Final rank pct | Learned rank pct | Heuristic rank |
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

## Disabled By Default

- `shadow_execution_enabled` is false.
- Shadow scoring is not allowed by this artifact.
- Production default is not allowed by this artifact.

## Not Shadow Execution / Not Production

- No shadow execution is enabled.
- No production/API/web/ranking/default behavior is changed.
- Execution readiness gates remain required before any shadow run.

## Caveats

- Implementation audit only; shadow execution remains disabled.
- No database access, scoring rerun, embedding generation, ranking run, label ingest, API/web change, or production change.
- Replay uses validation['candidate_work_scores'] exactly.
- Labels in validation rows are ignored by scoring.
