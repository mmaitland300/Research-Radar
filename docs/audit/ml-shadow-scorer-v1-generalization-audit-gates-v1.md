# ML Shadow Scorer v1 Generalization Audit Gates (ml-shadow-scorer-v1-generalization-audit-gates-v1)

## Executive Summary

This artifact evaluates the second-surface ml-shadow-scorer-v1 generalization audit against the preregistered offline gate contract. It does not implement runtime, enable online shadow execution, or authorize production/API behavior.

- Generalization audit gates passed: True
- Second-surface generalization passed: True
- Material lift gate passed: True
- Disabled-by-default runtime next stage allowed: True
- Recommended next stage: `implement_online_shadow_runtime_disabled_by_default`

## Evidence Summary

- Candidate pool: 528
- Confirmatory metric rows: 168
- Pool-only prior-overlap rows: 360
- Label counts: 94 positive / 74 negative
- Candidate SHA: `f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc`

## Gate Results

| Gate | Status | Rationale |
| --- | --- | --- |
| `G01_second_surface_identity` | pass | Run, family, snapshot, embedding, and SHA match the selected second surface. |
| `G02_candidate_and_label_coverage` | pass | Pool, metric rows, target counts, and conflict counts match the preregistered second-surface contract. |
| `G03_learned_probability_and_score_coverage` | pass | Every pool row has learned probability, rank percentile, and shadow score fields. |
| `G04_formula_replay_exact` | pass | Frozen 50/50 rank-fusion score replays exactly within tolerance and shadow ranks are complete. |
| `G05_no_leakage_or_refit` | pass | Labels are metric-only; no refit, supervised fit, tuning, or training rows are used. |
| `G06_prior_surface_exclusion` | pass | Prior-overlap rows are scored in the full pool but excluded from the confirmatory denominator. |
| `G07_material_lift_vs_heuristic` | pass | Material lift gate uses ROC-AUC/AP only; precision@k deltas are advisory. |
| `G08_shadow_prod_runtime_blocked` | pass | Online shadow, runtime implementation, production default, and API/web changes remain blocked. |
| `G09_production_readiness_still_separate` | pass | Production readiness remains research-only and separate from shadow generalization gates. |
| `G10_generalization_audit_gate_decision` | pass | Second-surface generalization gates pass iff G01 through G09 pass. |

## Material Lift

- Delta ROC-AUC: 0.100704
- Delta AP: 0.070175
- Thresholds: ROC-AUC >= 0.030000 OR AP >= 0.020000
- Audit material_lift_observed: True
- Recomputed material lift: True

## Advisory Precision@k

- `precision_at_5` delta: -0.200000
- `precision_at_10` delta: -0.200000
- `precision_at_20` delta: 0.050000
- Gate effect: reported_only_not_gate_failing

## Formula Replay

- Formula replay exact: True
- Max absolute formula delta: 0.000000
- Mismatched work count: 0
- Rank completeness passed: True

## Remaining Blockers

- `missing_generalization_audit_on_second_surface`: False
- `missing_generalization_audit_gates`: False
- `missing_online_shadow_implementation_disabled_by_default`: True
- `missing_shadow_runtime_isolation_verification`: True
- `missing_production_readiness_authorization`: True
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False
- `runtime_implementation_authorized`: False

## Caveats

- Gates pass, if observed, means second-surface generalization met the preregistered offline bar.
- Gates pass does not enable online shadow execution.
- Gates pass does not authorize production default or API/web behavior.
- The next allowed step is disabled-by-default runtime implementation only.
- Precision@k movements are reported as advisory; the preregistered material lift gate is ROC-AUC/AP.
- Discovery JSON may still be stale about missing_generalization_audit_gates until a later sync/rerun; this gates artifact is the source of truth for gate status.
