# Fresh Candidate Source Expansion Plan (ml-fresh-candidate-source-expansion-plan-v1)

## Executive Summary

No existing local source meets the fresh hybrid confirmatory candidate floor. This plan defines how to expand the candidate source before materialization, labeling, and any hybrid validation.

- **Sources considered:** 18
- **Best existing source:** `rank-3904fec89d`
- **Best confirmatory eligible works:** 44
- **Candidate gap:** 56
- **Recommended next stage:** `implement_or_run_fresh_product_candidate_source_build_v1`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Current Blocker

`rank-3904fec89d` has 44 confirmatory-eligible works after old-surface exclusion, below the policy minimum of 100. It also has 0 negative labeled works.

## Why This Is Source Expansion, Not ML Tuning

The blocker is upstream of model evidence: there is no sufficiently large fresh candidate denominator. The plan does not tune hybrid weights, lower thresholds, train models, or run validation.

## Allowed Strategies

| Priority | Strategy | Description |
| --- | --- | --- |
| primary | `create_newer_corpus_snapshot_and_candidate_run` | Preferred. New snapshot plus product-candidate ranking/scoring run after this plan; freeze before materialize. |
| secondary | `broaden_existing_snapshot_candidate_pool` | Expand product-candidate selection within newer or broader snapshot filters while preserving product-like scope. |
| required_for_label_readiness | `targeted_negative_candidate_collection` | Intentionally add product-plausible borderline/negative candidates for label balance; separate from hybrid scoring/training. |
| optional | `multi_family_candidate_source_with_declared_family_rules` | Allowed only if family rules are explicit and materializer reports per-family breakdown. |

## Forbidden Shortcuts

- reusing old 217 eval surface as confirmatory denominator (rank-ee2ba6c816 / SHA 21398640...)
- selecting rank-3904fec89d / 44-work surface as confirmatory-ready without expansion
- scoring hybrid arms before source materialization
- lowering policy thresholds to make current 44 pass
- post-hoc cherry-picking candidates by existing labels
- DB writes in this planning command
- shadow_scoring
- production_default_change
- bridge_default_change
- claiming confirmatory_validation_complete

## Candidate Generation Contract

- Future command name: `ml-fresh-product-candidate-source-build`
- Future commit scope: may use DB writes and ranking/candidate pipeline (first implementation leaving pure eval specs)
- Success criteria: confirmatory_eligible_work_count >= 100 after old-217 exclusion
- On success: `rerun_fresh_product_candidate_ranking_source_after_source_build`
- On failure: `revise_corpus_scope_or_candidate_filters`

## Labeling Implications

- Absolute labeled-work deficit after expansion: 80
- Coverage-only deficit on current 44: 7
- Negative work deficit: 20
- Distinct negative work deficit: 20
- Current best-source labels are positive-only; future source build must include a negative/borderline sampling plan.

## Frozen Hybrid Arms

- Primary: `hybrid_rank_mean_50_50`
- Secondary reporting: `hybrid_rank_mean_25_75_heuristic`
- No retuning on fresh eval labels.

## Next Stages

- `implement_or_run_fresh_product_candidate_source_build_v1` (primary)
- `rerun_fresh_product_candidate_ranking_source_after_source_build` (secondary)
- `rerun_fresh_eval_surface_hybrid_materialize_with_selected_source` (tertiary)
- `create_fresh_eval_labeling_worksheet_hybrid_v1` (quaternary)

## Not Shadow / Not Production

- Plan only; no candidate source is built in this artifact.
- Not hybrid validation and not live recommender validation.
- No database access, ranking creation, scoring, training, embeddings, or label import.
- Existing 44-work surface is not confirmatory-ready and must not be used for hybrid validation.
- Policy thresholds are not lowered by this plan.
- No shadow or production authorization.
