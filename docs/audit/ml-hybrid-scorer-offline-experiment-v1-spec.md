# Hybrid Scorer Offline Experiment Spec (ml-hybrid-scorer-offline-experiment-v1-spec)

## Executive Summary

This pre-registers the next offline hybrid scorer experiment after v3 gates. It does not run hybrid scoring, fit weights, or authorize shadow or production.

- **Eval work-set SHA:** `213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a`
- **Next authorized step:** `ml-hybrid-scorer-offline-experiment-v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Why Hybrid Is Next

Hybrid scoring is the natural next offline research step because final_score and learned probability may carry complementary signal.
The heuristic final_score is already strong, and the text-only holdout learned scorer roughly matches it without material lift. A hybrid arm may expose complementary signal, but this is not shadow readiness.

## Evidence Summary

| Metric | Heuristic | Holdout learned | Delta |
| --- | ---: | ---: | ---: |
| ROC-AUC | 0.804 | 0.805 | 0.001 |
| Average precision | 0.958 | 0.967 | 0.009 |
| Precision@5 | 1.000 | 1.000 | 0.000 |
| Precision@10 | 1.000 | 1.000 | 0.000 |
| Precision@20 | 1.000 | 1.000 | 0.000 |

Positive work prevalence: `0.876`.

## Material Lift Gaps

| Gap | Value |
| --- | ---: |
| ROC-AUC gap to material lift | 0.029 |
| Average precision gap to material lift | 0.011 |

## Allowed/Forbidden Feature Table

| Type | Items |
| --- | --- |
| Allowed features | final_score, audit_embedding_probability_work |
| Label-blind transforms | rank_percentile, z_score, min_max_scaling, logit_audit_embedding_probability_work |
| Forbidden | labels or derived targets as features, reviewer_notes, review_pool_variant, sample_reason, row_id, assignment as predictive feature, post-hoc transforms chosen using eval-label performance, supervised fitting or weight search on eval labels, DB/ranking writes |

Rank percentiles are computed on the full candidate pool, not only labeled eval works.

## Pre-Registered Hybrid Arms

| Arm | Formula |
| --- | --- |
| `heuristic_final_score_baseline` | `final_score` |
| `holdout_embedding_probability_baseline` | `audit_embedding_probability_work` |
| `hybrid_rank_mean_50_50` | `0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)` |
| `hybrid_rank_mean_75_25_heuristic` | `0.75 * rank_pct(final_score) + 0.25 * rank_pct(audit_embedding_probability_work)` |
| `hybrid_rank_mean_25_75_heuristic` | `0.25 * rank_pct(final_score) + 0.75 * rank_pct(audit_embedding_probability_work)` |

## Candidate/Eval Policy

- Candidate score transforms are computed over the full scoring v3 candidate pool.
- Metrics are computed only on eval-assignment labeled works.
- All arms must use the same eval work set and label denominators.
- Unlabeled candidate works may appear in score-distribution summaries, not label-metric denominators.
- Positive-heavy eval means P@k is advisory; ROC-AUC/AP carry more signal.

## Future Experiment Contract

- **Command:** `ml-hybrid-scorer-offline-experiment`
- Compute all pre-registered arms exactly.
- Use no supervised fitting.
- Compute rank percentiles on the full candidate pool.
- Report heuristic, learned, and hybrid metrics on identical eval works.

## Future Gates Sketch

- **Command:** `ml-hybrid-scorer-metric-gates`
- Hybrid material lift requires ROC-AUC delta >= 0.03 or AP delta >= 0.02 versus heuristic.
- P@10 non-regression is advisory when both arms are saturated at 1.0.
- Best arm on seen eval is exploratory only: True
- If no lift: `collect_labels_or_features_or_new_eval_surface`.

## Forbidden Designs

- supervised hybrid on eval labels
- choosing weights after seeing v3/gates metrics and claiming validation
- picking the best pre-registered arm after seeing eval metrics and treating that as confirmatory validation
- using product-candidate eval labels to train a combiner
- shadow deployment
- production default change
- silent label conflict resolution

## Not Shadow / Not Production Caveats

- Missing hybrid experiment results: True
- Missing hybrid metric gates: True
- Missing `ml-shadow-scorer-v1`: True
- No production model artifact: True

- Spec only; no scoring executed.
- v3 eval has already been observed, so future hybrid run is diagnostic/exploratory on this same surface unless a new holdout is defined.
- Single-reviewer audit labels.
- One ranking run/family.
- Positive-heavy P@k.
- Not live recommender validation.
- Not shadow readiness.
- No production authorization.
