# ML Production Readiness Plan v1

## Executive Summary

- **overall_status:** `research_only`
- **rationale:** research_only because inputs are valid and good_or_acceptable has in-pool signal, but production gates remain unsatisfied.
- **primary target:** `good_or_acceptable` for offline ranker research only.
- **deferred target:** `surprising_or_useful` for rubric and labeling work only.
- No production ranking, shadow scoring, or default change is authorized by this plan.

## Gate Checklist

| gate_id | status | blocking | required_for | next_action |
| --- | --- | --- | --- | --- |
| `G1_target_selection` | `partial` | `{"offline_ranker_experiment": false, "production_default": true, "shadow_scoring": false}` | offline_ranker_experiment, production_default | proceed with offline ranker research for good_or_acceptable only |
| `G2_label_volume_and_balance` | `partial` | `{"offline_ranker_experiment": false, "production_default": true, "shadow_scoring": false}` | offline_ranker_research_only, production_default | collect labels in sparse/imbalanced pools per label_gaps |
| `G3_multi_source_transfer` | `partial` | `{"offline_ranker_experiment": false, "production_default": true, "shadow_scoring": false}` | production_default | improve cross-source labels before treating transfer as production evidence |
| `G4_rubric_adjudication` | `not_started` | `{"offline_ranker_experiment": false, "production_default": true, "shadow_scoring": false}` | production_default | write and apply a multi-reviewer/adjudication policy before production claims |
| `G5_split_policy_artifact` | `not_started` | `{"offline_ranker_experiment": true, "production_default": true, "shadow_scoring": false}` | offline_ranker_experiment, production_default | create ml-label-split-policy-v1 before offline ranker gate experiments |
| `G6_candidate_pool_definition` | `not_started` | `{"offline_ranker_experiment": true, "production_default": true, "shadow_scoring": false}` | offline_ranker_experiment, production_default | define a product-matched candidate pool before offline ranker experiments |
| `G7_offline_metric_gates` | `not_started` | `{"offline_ranker_experiment": false, "production_default": true, "shadow_scoring": true}` | shadow_scoring, production_default | define and pass offline top-k workflow metrics before shadow scoring |
| `G8_shadow_mode_contract` | `not_started` | `{"offline_ranker_experiment": false, "production_default": true, "shadow_scoring": true}` | shadow_scoring, production_default | write ml-shadow-scorer-v1 only after offline gates pass |
| `G9_leakage_controls` | `not_started` | `{"offline_ranker_experiment": true, "production_default": true, "shadow_scoring": true}` | offline_ranker_experiment, shadow_scoring, production_default | document leakage controls in split and experiment artifacts |
| `G10_production_rollout` | `not_started` | `{"offline_ranker_experiment": false, "production_default": true, "shadow_scoring": false}` | production_default | do not change production defaults until all prior gates and human approval are complete |

## Target Readiness

| target | status | allowed_next_stage | production_eligible | rationale |
| --- | --- | --- | --- | --- |
| `good_or_acceptable` | `primary_candidate` | `offline_ranker_research_only` | `False` | good_or_acceptable has an in-pool text signal and is not flagged as weak for external/blind transfer, so it is the only v1 target allowed to proceed to offline ranker research. |
| `surprising_or_useful` | `deferred` | `rubric_and_labeling_only` | `False` | surprising_or_useful is flagged for weak external/blind transfer and transfer inconsistency, so v1 limits it to rubric clarification and additional labeling. |

## Label Gaps

| priority | target | pool | pos | neg | null | action |
| --- | --- | --- | ---: | ---: | ---: | --- |
| `P1` | `surprising_or_useful` | `ml_external_near_miss_audit + ml_blind_snapshot_audit` | 139 | 41 | 0 | Clarify the surprising_or_useful rubric and collect more balanced labels across external, blind, and rank-shaped sources. |
| `P2` | `good_or_acceptable` | `ml_external_near_miss_audit + ml_blind_snapshot_audit` | 127 | 53 | 0 | Expand cross-source labeling for good_or_acceptable and use future offline ranker experiments only as research probes. |
| `P3` | `good_or_acceptable` | `(null)` | 65 | 0 | 0 | Add negatives or mark this pool unsuitable for split/eval until balanced. |
| `P3` | `good_or_acceptable` | `bridge_eligible_only` | 19 | 1 | 0 | Add negatives or mark this pool unsuitable for split/eval until balanced. |
| `P3` | `good_or_acceptable` | `full_family_top_k` | 39 | 1 | 0 | Add negatives or mark this pool unsuitable for split/eval until balanced. |
| `P3` | `good_or_acceptable` | `ml_hard_negative_audit` | 7 | 0 | 0 | Add negatives or mark this pool unsuitable for split/eval until balanced. |
| `P3` | `surprising_or_useful` | `(null)` | 65 | 0 | 0 | Add negatives or mark this pool unsuitable for split/eval until balanced. |
| `P3` | `surprising_or_useful` | `bridge_eligible_only` | 18 | 2 | 0 | Add negatives or mark this pool unsuitable for split/eval until balanced. |
| `P3` | `surprising_or_useful` | `full_family_top_k` | 36 | 4 | 0 | Add negatives or mark this pool unsuitable for split/eval until balanced. |
| `P3` | `surprising_or_useful` | `ml_hard_negative_audit` | 6 | 1 | 0 | Add negatives or mark this pool unsuitable for split/eval until balanced. |

## No-Go Conditions

- Training alone does not authorize production.
- Single-reviewer audit labels are insufficient for default ranking.
- surprising_or_useful is not eligible as a v1 production target.
- good_or_acceptable is offline ranker research only, not production-eligible until gates satisfied.
- Shadow scoring cannot start before offline gates pass.
- Production default cannot change without shadow evidence and human approval.
- Silent deduplication or conflict resolution of paper_id labels is forbidden.
- Rank-shaped-only evidence cannot justify production ML.

## Ordered Next Artifacts

- `ml-transfer-gap-review-worksheet`: Collect targeted labels for weak transfer and sparse pools.
- `ml-label-split-policy-v1`: Define deterministic split eligibility, seed, leakage controls, and conflict-policy references.
- `ml-offline-ranker-experiment-v1`: Evaluate a production-candidate offline ranker against heuristic baselines on frozen candidate pools.
- `ml-shadow-scorer-v1`: Define no-user-impact shadow scoring contract after offline gates pass.
- `production flag change`: Human-approved default or feature-flag change after all gates pass.

## Not Validation / Not Production Recommender Test

- Not validation.
- Single-reviewer audit labels.
- Gates are prerequisites not guarantees.
- No production ranking implication.
- No new training/embeddings/ranking/splits.
- Observation-level duplicates/conflicts preserved.
- good_or_acceptable research-only.
- surprising_or_useful deferred for production.
