# Fresh Eval Surface Policy For Hybrid Validation (ml-fresh-eval-surface-policy-hybrid-v1)

## Executive Summary

This policy defines what counts as a genuinely fresh product-candidate eval surface for confirming the hybrid scorer result. It does not materialize a pool, score candidates, train, label, authorize shadow, or authorize production.

- **Status:** `blocked_for_confirmatory_until_surface_materialized`
- **Disallowed eval work-set SHA:** `213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a`
- **Primary fresh-surface strategy:** `new_snapshot_new_product_candidate_run`
- **Frozen primary hybrid arm:** `hybrid_rank_mean_50_50`

## Why A Fresh Surface Is Required

Hybrid scorer metric gates v1 showed material lift on an already-observed 217-work eval surface, but confirmatory validation remains false. The old 217-work surface cannot be used as confirmatory evidence.

## Disallowed 217-Work Surface Details

| Field | Value |
| --- | --- |
| Surface ID | `product_candidate_eval_surface_rank-ee2ba6c816_emerging_v3` |
| Candidate works | 217 |
| Eval work-set SHA | `213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a` |
| Ranking run | `rank-ee2ba6c816` |
| Family | `emerging` |
| Confirmatory use | `disallowed` |
| Allowed use | `regression_smoke_or_historical_comparison_only` |

Overlapping works in a future pool must be tagged `previous_eval_overlap` and excluded from confirmatory metric denominators.

## Primary Fresh-Surface Path

- Future materialization uses a new corpus snapshot and a new product-candidate ranking/scoring run produced after this policy.
- The materialized candidate work-set SHA must differ from the disallowed eval_work_set_sha256.
- Confirmatory metric rows must be canonical works not in the disallowed 217-work set.
- The surface must be frozen into an artifact before any hybrid validation scoring runs.

## Label Thresholds

| Threshold | Value |
| --- | ---: |
| `minimum_candidate_work_count` | 100 |
| `minimum_confirmatory_labeled_work_count` | 100 |
| `minimum_confirmatory_positive_work_count` | 50 |
| `minimum_confirmatory_negative_work_count` | 20 |
| `minimum_confirmatory_label_coverage_rate` | 0.6 |
| `minimum_distinct_negative_work_count` | 20 |

Thresholds apply after excluding old 217-work overlap from confirmatory metric denominators.

## Frozen Hybrid Arms

- Primary confirmatory arm: `hybrid_rank_mean_50_50`
- Secondary reporting arm: `hybrid_rank_mean_25_75_heuristic`
- Baselines: `heuristic_final_score_baseline`, `holdout_embedding_probability_baseline`
- No re-tuning weights on fresh labels.
- No selecting a new best arm on fresh labels and calling it confirmatory.

## Gate Linkage

- Inherits material lift thresholds from `ml-hybrid-scorer-metric-gates-v1`.
- Material lift remains ROC-AUC delta >= 0.03 OR AP delta >= 0.02.
- Candidate coverage, label coverage, class balance, overlap checks, metrics, material lift, and confirmatory validation must be rerun on the fresh surface.
- Old-surface lift, best-arm metrics, ROC-AUC/AP values, top-k values, and confirmatory validation must not be inherited.

## Blocked Actions

- shadow_scoring
- production_default_change
- bridge/default ranking changes
- public copy claiming production readiness
- model/scorer deployment
- silent label conflict resolution
- using old 217-work surface for confirmatory hybrid validation

## Future Artifact Chain

- Materialization command: `ml-fresh-eval-surface-hybrid-materialize`
- Outputs: `docs/audit/ml-fresh-eval-surface-hybrid-v1.json`, `docs/audit/ml-fresh-eval-surface-hybrid-v1.md`
- Future scoring command: `ml-hybrid-validation-on-fresh-surface`
- Future gates command: `ml-hybrid-validation-metric-gates`

## Not Shadow / Not Production Caveats

- Shadow allowed by this policy: False
- Production default allowed by this policy: False
- Confirmatory validation complete: False

- Policy only; no fresh surface materialized yet.
- Not live recommender validation.
- Hybrid lift so far is exploratory on an already-seen surface.
- Single-reviewer audit labels remain a limitation.
- Future surface must satisfy label coverage and negative-count thresholds.
- No shadow or production authorization.
