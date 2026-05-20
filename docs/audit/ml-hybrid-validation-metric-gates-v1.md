# Hybrid Validation Metric Gates (ml-hybrid-validation-metric-gates-v1)

## Executive Summary

This deterministic evaluator checks the frozen primary hybrid arm on the fresh 143-work confirmatory denominator. Passing these gates authorizes only drafting a shadow scorer spec; it does not authorize shadow execution or production default.

- **Primary confirmatory arm:** `hybrid_rank_mean_50_50`
- **Primary material lift passed:** True
- **Confirmatory validation passed:** True
- **Recommended next stage:** `draft_ml_shadow_scorer_v1_spec`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Gate Checklist

| Gate | Status | Rationale |
| --- | --- | --- |
| G01_input_scope Input Scope | pass | The gates must evaluate the fresh-surface validation v1 artifact against policy v1. |
| G02_fresh_surface_readiness Fresh Surface Readiness | pass | Fresh validation can only be confirmed on a materialized-ready surface with candidate and label thresholds met. |
| G03_leakage_and_freshness Leakage And Freshness | pass | Confirmatory metrics must exclude the old eval surface and avoid fitting, retuning, or scorer refit. |
| G04_frozen_primary_arm Frozen Primary Arm | pass | The primary confirmatory decision must use the frozen 50/50 arm, not a post-hoc best arm. |
| G05_metric_completeness Metric Completeness | pass | Heuristic, holdout learned, and primary hybrid metrics must all be present before gate evaluation. |
| G06_primary_material_lift_vs_heuristic Primary Material Lift Vs Heuristic | pass | The frozen primary hybrid arm must beat heuristic by the policy ROC-AUC or AP material-lift threshold. |
| G07_primary_top_k_non_regression Primary Top-K Non-Regression | pass | Top-k precision should not regress for the primary hybrid arm on the fresh surface. |
| G08_best_arm_exploratory_only Best Arm Exploratory Only | pass | Best-arm reporting remains exploratory and cannot override the frozen primary arm. |
| G09_confirmatory_validation_decision Confirmatory Validation Decision | pass | Confirmatory validation passes only if all scope, readiness, leakage, frozen-arm, metric, lift, and interpretation gates pass. |
| G10_shadow_and_production_blockers Shadow And Production Blockers | pass | Passing gates may authorize only drafting a shadow spec; execution and production remain blocked. |
| G11_production_readiness_alignment Production Readiness Alignment | pass | The production readiness plan must still block production default changes. |

## Primary Hybrid Vs Heuristic

| Metric | Heuristic | Primary hybrid | Delta |
| --- | ---: | ---: | ---: |
| ROC-AUC | 0.8252 | 0.9103 | 0.0851 |
| Average precision | 0.6494 | 0.8617 | 0.2123 |
| P@5 | 0.6000 | 1.0000 | 0.4000 |
| P@10 | 0.5000 | 1.0000 | 0.5000 |
| P@20 | 0.5000 | 0.9500 | 0.4500 |

## Best Arm Note

- Best by ROC-AUC: `hybrid_rank_mean_50_50`.
- Best by AP: `hybrid_rank_mean_50_50`.
- Best-arm reporting is exploratory only; the confirmatory decision is based on `hybrid_rank_mean_50_50`.

## Not Shadow / Not Production

- Shadow scoring remains blocked.
- Production default remains blocked.
- A future `ml-shadow-scorer-v1` spec is the next authorized drafting step only if these gates pass.

## Caveats

- Not live recommender validation.
- Fresh confirmatory surface; old 217 overlaps are excluded from confirmatory metrics.
- Frozen holdout scorer v2 applied without refit.
- Single-reviewer audit labels.
- Primary confirmatory arm is fixed at hybrid_rank_mean_50_50; best-arm selection remains exploratory.
- Passing these gates authorizes only drafting a shadow scorer spec.
- No shadow execution, production default, API/web change, or model deployment is authorized.
