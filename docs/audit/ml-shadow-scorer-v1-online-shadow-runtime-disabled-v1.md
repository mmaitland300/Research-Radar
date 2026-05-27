# ml-shadow-scorer-v1 Online Shadow Runtime Disabled (ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1)

## Executive Summary

This artifact records an inert, disabled-by-default runtime implementation for ml-shadow-scorer-v1. It does not enable online shadow execution, write shadow tables, integrate API/web paths, or change production defaults.

- Runtime implementation present: True
- Runtime default state: `off`
- Feature flag: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED`
- Runtime execution authorized: False
- Last disabled run status: `skipped_runtime_disabled`
- Recommended next stage: `run_ml_shadow_scorer_v1_runtime_isolation_verification_v1`

## Feature Flag Behavior

- On values: `1`, `true`, `on`, `yes`, `enabled`
- Off values: ``, `0`, `false`, `off`, `no`, `disabled`
- Unset, empty, unknown, and all non-on values are treated as off.

## Runtime Contract

- Entry point: `run_ml_shadow_scorer_v1_online_shadow_runtime`
- Required input fields: `canonical_openalex_work_id`, `final_score`, `audit_embedding_probability_work`, `ranking_run_id`, `family`
- Identity fields checked when present: `candidate_pool_work_set_sha256`, `corpus_snapshot_version`, `embedding_version`, `family`, `ranking_run_id`
- Partial scoring allowed: False
- Skip on incomplete coverage: True
- Writes performed: False

## Forbidden Label Fields

- `bridge_like_label`
- `good_or_acceptable`
- `holdout_assignment`
- `holdout_assignment_version`
- `holdout_set`
- `holdout_split`
- `label_any_positive`
- `novelty_label`
- `relevance_label`
- `review_pool_variant`
- `reviewer_notes`
- `sample_reason`
- `split`
- `train_eval_split`

## Remaining Blockers

- `missing_generalization_audit_on_second_surface`: False
- `missing_generalization_audit_gates`: False
- `missing_online_shadow_implementation_disabled_by_default`: False
- `missing_shadow_runtime_isolation_verification`: True
- `missing_production_readiness_authorization`: True
- `online_shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False
- `runtime_implementation_authorized`: False

## Future Verification

- Future artifact: `docs/audit/ml-shadow-scorer-v1-runtime-isolation-verification-v1.json`
- Runtime isolation verification is still required before any future online shadow execution.

## Caveats

- Runtime implementation is present but disabled by default.
- This artifact does not authorize runtime execution, online shadowing, DB writes, API/web behavior, or production default changes.
- Runtime scoring can only use supplied read-only rows with final_score and audit_embedding_probability_work.
- Incomplete learned-probability coverage skips the entire run; no partial shadow scoring is produced.
- Labels and holdout assignment fields are rejected as scoring inputs.
- Runtime isolation verification remains required before any future online shadow execution.
