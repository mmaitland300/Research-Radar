# Hybrid Scorer Metric Gates (ml-hybrid-scorer-metric-gates-v1)

## Executive Summary

This deterministic evaluator checks whether pre-registered hybrid arms clear material lift on the already-seen v3 eval surface. Passing these gates does not authorize shadow or production.

- **Hybrid material lift passed:** True
- **Confirmatory validation passed:** False
- **Recommended next stage:** `create_fresh_eval_surface_for_hybrid_validation_v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Three-Decision Table

| Decision | Result | Meaning |
| --- | --- | --- |
| Material lift | True | At least one pre-registered hybrid arm beats heuristic by ROC-AUC >= 0.03 or AP >= 0.02. |
| Best-arm exploratory | True | Best-arm choice happened on an already-seen eval surface. |
| Confirmatory validation | False | Fresh eval surface has not been defined or evaluated. |

## Gate Checklist

| Gate | Status | Rationale |
| --- | --- | --- |
| G01_input_scope Input Scope | pass | The hybrid gates must evaluate the v1 experiment bound to scoring v3 and product-candidate gates v3. |
| G02_no_supervised_fit No Supervised Fit | pass | The hybrid experiment must be label-blind rank fusion, with no fitting or eval-label weight tuning. |
| G03_pre_registered_arms_executed Pre-Registered Arms Executed | pass | The executed arms must exactly match the pre-registered baseline and hybrid formulas. |
| G04_hybrid_material_lift_vs_heuristic Hybrid Material Lift Vs Heuristic | pass | Only pre-registered hybrid arms can satisfy the material-lift bar; baselines do not count. |
| G05_best_arm_exploratory_only Best Arm Exploratory Only | pass | Best-arm selection on the already-seen surface must not be treated as confirmatory validation. |
| G06_confirmatory_validation_status Confirmatory Validation Status | not_applicable | Material lift on the known surface is exploratory; confirmatory validation requires a new eval surface/holdout policy. |
| G07_top_k_saturation_advisory Top-K Saturation Advisory | advisory_warn | P@10 saturation on a positive-heavy surface is advisory and does not fail gates. |
| G08_positive_prevalence_advisory Positive Prevalence Advisory | advisory_warn | High positive prevalence can inflate P@k and makes top-k evidence advisory. |
| G09_shadow_blockers_documented Shadow Blockers Documented | pass | Even with hybrid lift, this artifact keeps shadow blocked until confirmatory validation and a shadow contract exist. |
| G10_production_readiness_alignment Production Readiness Alignment | pass | The production readiness plan must keep production/default behavior blocked. |

## Best Hybrid Vs Heuristic

| Metric | Heuristic | Best hybrid | Delta |
| --- | ---: | ---: | ---: |
| ROC-AUC | 0.804 | 0.846 | 0.042 |
| Average precision | 0.958 | 0.974 | 0.016 |
| P@10 | 1.000 | 1.000 | 0.000 |

Best hybrid by ROC-AUC: `hybrid_rank_mean_50_50`. Best hybrid by AP: `hybrid_rank_mean_50_50`.

## Arms Passing Lift

- `hybrid_rank_mean_50_50`
- `hybrid_rank_mean_25_75_heuristic`

## Exploratory Warning

This is an encouraging offline diagnostic on a known eval surface. It is not confirmatory validation, and the chosen best arm must be checked on a fresh eval surface before any shadow-spec work.

## Not Shadow / Not Production

- Shadow scoring allowed: False
- Production default allowed: False
- Missing `ml-shadow-scorer-v1` contract.
- No production model artifact.

## Recommended Next Stage

`create_fresh_eval_surface_for_hybrid_validation_v1`

## Caveats

- Not live recommender validation.
- Hybrid lift on already-seen eval surface.
- Best-arm selection exploratory.
- Single reviewer.
- One ranking run/family.
- Positive-heavy P@k.
- No shadow/production authorization.
