# ML Shadow Scorer v1 Online Shadow Policy (ml-shadow-scorer-v1-online-shadow-policy)

## Executive Summary

This policy defines the minimum rules for any future online shadow path for `ml-shadow-scorer-v1`. It does not implement runtime shadowing, enable shadow execution, or authorize production behavior.

- Online shadow execution policy defined: True
- Online shadow execution enabled: False
- Runtime implementation authorized: False
- Production default allowed: False
- Recommended next stage: `draft_ml_shadow_scorer_v1_generalization_audit_v1`

## Evidence Chain

- Audit output gates passed: True
- Offline audit output ready: True
- Validation replay exact: True
- Production plan still blocked: True

## Validation Snapshot Scope

- Ranking run: `rank-9f4b2a2084`
- Family: `emerging`
- Corpus snapshot: `source-snapshot-fresh-hybrid-v1-20260518`
- Embedding version: `fresh-hybrid-text-embedding-v1`
- Candidate pool SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`
- Evidence applies only to this scope: True
- Formula generalization beyond validated surface asserted: False

## Terminology

| Term | Value | Meaning |
| --- | --- | --- |
| `shadow_scoring_allowed` | False | Umbrella authorization for any shadow scoring path. |
| `shadow_audit_execution_allowed` | True | Offline JSON audit only from prior gates. |
| `online_shadow_execution_enabled` | False | Runtime path on. |
| `runtime_implementation_authorized` | False | Authorization to implement online runtime shadowing. |

## Generalization Requirement Before Runtime

- Second-surface generalization audit must pass: True
- Runtime implementation authorized by this policy: False
- Required next artifacts:
  - `draft_ml_shadow_scorer_v1_generalization_audit_v1`
  - `audit_ml_shadow_scorer_v1_on_second_fresh_surface`
  - `ml-shadow-scorer-v1-generalization-audit-gates`

## Online Shadow Scope

- Future-only and not authorized now.
- Shadow results must not affect user-visible ranking, API responses, bridge defaults, or production defaults.
- Results are audit/monitoring only.

## Data Dependency Contract

- Final score source: production ranking outputs, read-only
- Learned probability source: pre-existing audit_embedding_probability_work or an upstream approved artifact that applied frozen ml-offline-audit-embedding-scorer-v2 to pre-existing embeddings, read-only
- Embedding generation at shadow time allowed: False
- Skip on incomplete coverage: True

## Runtime Isolation

- Feature flag: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED`
- Feature flag default off: True
- Write mode forbidden until runtime isolation verification passes: True

## Allowed Writes

- isolated shadow/audit table
- isolated shadow/audit artifact

## Forbidden Writes

- ranking_runs production/default pins
- paper_scores used by production/default ranking
- API-visible result tables
- production config/env/default bridge weights
- label datasets
- training/scorer artifacts
- embedding storage writes
- user-visible UI/API paths

## Observability

- `component_coverage`: True
- `missing_learned_probability`: True
- `score_distributions`: True
- `top_k_overlap_with_heuristic`: True
- `rank_displacement`: True
- `family_counts`: True
- `output_completeness`: True
- `runtime_errors`: True
- `latency`: True
- `skipped_candidates_and_reasons`: True
- `skipped_ranking_run_records`: True
- `write_counts_by_isolated_target`: True

## Disable And Rollback

- Disable switch: `ML_SHADOW_SCORER_V1_RUNTIME_ENABLED=off`
- Production ranking unaffected when on or off: True

## Separation From Production Default

- Future online shadow gates do not set production default allowed: True
- Production default authorization chain is separate: True

## Future Runtime Verification Requirements

- This policy implemented exactly
- Generalization audit on second surface passed first
- Feature flag default off
- Shadow writes confined to isolated namespace
- Zero production ranking writes
- API/web unchanged
- Observability complete
- Disable path tested
- Skip-on-incomplete-coverage tested
- snapshot/family/run scope on every record

## Remaining Blockers

- `missing_online_shadow_execution_policy`: False
- `missing_generalization_audit_on_second_surface`: True
- `missing_online_shadow_implementation_disabled_by_default`: True
- `missing_shadow_runtime_isolation_verification`: True
- `missing_production_readiness_authorization`: True
- `shadow_execution_enabled`: False
- `shadow_scoring_allowed`: False
- `production_default_allowed`: False
- `online_shadow_execution_enabled`: False
- `runtime_implementation_authorized`: False

## Caveats

- Policy document only; no online shadow execution is implemented or enabled.
- Validation evidence is snapshot-bound to rank-9f4b2a2084 / emerging / source-snapshot-fresh-hybrid-v1-20260518.
- Runtime implementation is explicitly deferred until a second-surface generalization audit passes.
- No API/web behavior, production default, user-visible ranking, training, embedding generation, or label ingest is authorized.
