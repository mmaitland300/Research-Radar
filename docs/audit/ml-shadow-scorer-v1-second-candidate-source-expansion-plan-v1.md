# ML Shadow Scorer v1 Second Candidate Source Expansion Plan (ml-shadow-scorer-v1-second-candidate-source-expansion-plan-v1)

## Executive Summary

Local discovery found no distinct second surface that meets the 100-work confirmatory-eligible minimum. This plan defines source expansion only; it is not ML retuning, scorer execution, runtime work, shadow execution, or production authorization.

- Status source: `blocked_no_candidate_source_meets_minimum`
- Sources considered: 19
- Best distinct source: `rank-3904fec89d`
- Best confirmatory-eligible works: 43 / 100
- Candidate gap: 57
- Recommended next stage: `implement_or_run_second_fresh_candidate_source_build_for_shadow_generalization_v1`

## Current Blocker

The best distinct source, `rank-3904fec89d`, has 59 pool works and 43 confirmatory-eligible works after excluding prior surfaces. The policy minimum is 100, leaving a gap of 57.

## Why This Is Expansion, Not ML Tuning

The blocker is source supply. The first validated surface remains non-reusable for generalization evidence, and no policy threshold is lowered.

## Requirements

- `minimum_confirmatory_eligible_work_count_after_exclusions`: 100
- `exclude_old_217_eval_surface`: {'required': True, 'work_set_sha256': '213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a'}
- `exclude_first_validated_surface`: {'required': True, 'ranking_run_id': 'rank-9f4b2a2084', 'candidate_pool_work_set_sha256': '927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6'}
- `ranking_run_id_must_differ_from`: rank-9f4b2a2084
- `candidate_pool_work_set_sha256_must_differ_from`: 927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6
- `prefer_newer_or_broader_corpus_snapshot_version`: True
- `canonical_openalex_work_ids_required`: True
- `freeze_candidate_source_metadata_before_scoring_or_audit`: True
- `full_final_score_coverage_required`: True
- `must_later_support_full_audit_embedding_probability_work_coverage`: True
- `labels_must_not_drive_candidate_selection`: True

## Allowed Strategies

| Priority | Strategy | Description |
| --- | --- | --- |
| primary | `create_newer_corpus_snapshot_and_candidate_run` | Preferred. Create or select a newer corpus snapshot and product-candidate ranking run, then rerun discovery. |
| secondary | `broaden_existing_snapshot_candidate_pool` | Broaden product-plausible filters within an existing snapshot while preserving source provenance and final_score coverage. |
| label_readiness_support | `targeted_borderline_and_negative_candidate_collection` | Deliberately include product-plausible borderline and negative-oriented candidates so later labels are not positive-only. |
| optional | `multi_family_candidate_source_with_declared_family_rules` | Optional only; emerging remains the default and any multi-family source needs explicit family accounting. |

## Forbidden Strategies

- reusing rank-9f4b2a2084 as the second surface
- reusing candidate SHA 927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6
- lowering thresholds to make rank-3904fec89d pass
- cherry-picking candidates by labels
- scorer execution before surface re-selection
- embedding generation or learned scorer application inside this plan
- online shadow/runtime/prod/API changes

## Learned Probability And Labeling Follow-Ons

- Full learned-probability coverage is required later, but this plan does not generate probabilities or embeddings.
- A pool of at least 100 confirmatory-eligible works still may need labels before audit execution.
- Labels are metric-only and never scoring features.

## Planned Commit Sequence

1. feat(eval): add second fresh candidate source expansion plan for shadow generalization v1
2. feat(eval): implement or run second fresh candidate source build for shadow generalization v1
3. feat(eval): materialize or select second fresh surface for shadow generalization v1 (rerun discovery)
4. feat(eval): create second-surface labeling plan if selected_needs_labels
5. feat(eval): create second-surface learned-probability coverage plan if selected_needs_learned_probability_coverage
6. feat(eval): audit ml-shadow-scorer-v1 on second fresh surface
7. feat(eval): add ml-shadow-scorer-v1 generalization audit gates
8. feat(eval): implement online shadow runtime disabled by default (only after generalization gates pass)

## Blockers

- `missing_second_fresh_candidate_source_expansion_plan_v1`: False
- `missing_second_fresh_candidate_source`: True
- `missing_generalization_audit_on_second_surface`: True
- `missing_generalization_audit_gates`: True
- `missing_online_shadow_implementation_disabled_by_default`: True
- `missing_shadow_runtime_isolation_verification`: True
- `missing_production_readiness_authorization`: True
- `runtime_implementation_authorized`: False
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False

## Caveats

- Plan only; no candidate source build, ranking creation, scorer execution, embeddings, runtime, shadow/prod, or API/web changes.
- This is source expansion because local DB inventory lacks a qualifying second surface, not because the hybrid formula failed on surface one.
- Policy thresholds are not lowered by this plan.
- Labels remain metric-only and must not be used for candidate selection.
