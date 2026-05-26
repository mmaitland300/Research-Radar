# Second-Surface ml-shadow-scorer-v1 Generalization Audit (ml-shadow-scorer-v1-second-surface-generalization-audit-v1)

## Executive Summary

This artifact audits the frozen ml-shadow-scorer-v1 formula on the selected second fresh surface. It prepares evidence for later gates only; it does not pass gates or authorize runtime, online shadowing, API/web behavior, or production defaults.

- Ranking run: `rank-83787b91ef`
- Family: `emerging`
- Candidate pool: 528
- Confirmatory metric works: 168
- Candidate SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`
- Recommended next stage: `run_ml_shadow_scorer_v1_generalization_audit_gates_v1`

## Confirmatory Denominator

- Joined v11 labels: 168 (94 positive, 74 negative)
- Review pool variant: `ml_shadow_scorer_second_surface_generalization_v1`
- Metric denominator: v11 shadow-generalization confirmatory worksheet rows only
- Labels used for scoring: false

## Learned Probability Coverage

- Coverage: 528 / 528
- Missing probabilities: 0

## Metrics Vs Heuristic

| Arm | ROC-AUC | AP | P@5 | P@10 | P@20 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `heuristic_final_score_baseline` | 0.594595 | 0.643766 | 1.000000 | 1.000000 | 0.700000 |
| `hybrid_rank_mean_50_50` | 0.695299 | 0.713941 | 0.800000 | 0.800000 | 0.750000 |

## Material Lift

- Delta ROC-AUC: 0.100704
- Delta AP: 0.070175
- Delta P@5 / P@10 / P@20: -0.200000 / -0.200000 / 0.050000
- Material lift observed: True
- Generalization gates passed: false (gates not run in this artifact)

## Top 20 Shadow Preview

| Rank | Work | Score | Heuristic rank | Label metric row |
| ---: | --- | ---: | ---: | --- |
| 1 | `W4408772031` | 0.996205 | 4 | False |
| 2 | `W4402645135` | 0.979127 | 9 | False |
| 3 | `W4410091503` | 0.977230 | 10 | False |
| 4 | `W4401909510` | 0.962049 | 24 | True |
| 5 | `W7119099299` | 0.960152 | 36 | False |
| 6 | `W4416267785` | 0.958254 | 35 | False |
| 7 | `W4407236737` | 0.953510 | 18 | False |
| 8 | `W4411141958` | 0.941176 | 30 | False |
| 9 | `W4414799845` | 0.938330 | 19 | False |
| 10 | `W4409215215` | 0.936433 | 1 | False |
| 11 | `W7116976483` | 0.931689 | 16 | False |
| 12 | `W7134122976` | 0.924099 | 50 | False |
| 13 | `W4409967775` | 0.923150 | 7 | False |
| 14 | `W4412780451` | 0.922201 | 48 | False |
| 15 | `W4409215261` | 0.920304 | 3 | False |
| 16 | `W7128595024` | 0.917457 | 37 | False |
| 17 | `W4415316343` | 0.907021 | 43 | False |
| 18 | `W4401401564` | 0.904175 | 41 | True |
| 19 | `W4412900768` | 0.898482 | 13 | False |
| 20 | `W4415947443` | 0.894687 | 22 | False |

## Top-k And Rank Displacement

- Top-20 overlap with heuristic: 10
- Mean absolute rank displacement: 74.200758
- Median absolute rank displacement: 59.000000
- P90 absolute rank displacement: 161.000000
- Max absolute rank displacement: 239.000000

## Leakage Report

- Old 217 overlap excluded from confirmatory metrics: True
- First validated surface overlap excluded from confirmatory metrics: True
- Full-pool prior overlap counts: old 217 = 217, first surface = 358, combined = 360
- Train rows used: 0
- Scorer refit used: False
- Labels used for scoring: False

## Remaining Blockers

- `missing_generalization_audit_on_second_surface`: False
- `missing_generalization_audit_gates`: True
- `missing_online_shadow_implementation_disabled_by_default`: True
- `missing_shadow_runtime_isolation_verification`: True
- `missing_production_readiness_authorization`: True
- `runtime_implementation_authorized`: False
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False

## Caveats

- Offline audit artifact only; this does not pass generalization gates.
- The frozen ml-shadow-scorer-v1 formula is applied to committed second-surface probabilities only.
- Labels are used only for confirmatory metric evaluation and never for scoring, ranks, weights, or row ordering.
- Prior-surface overlap rows remain scored in the full pool but are excluded from confirmatory metrics.
- No database access, DB writes, ranking, embedding generation, learned scorer refit, label ingest, shadow runtime, API/web, or production/default changes.
