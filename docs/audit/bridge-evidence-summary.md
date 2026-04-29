# Bridge Evidence Summary

This index summarizes the current evidence-backed bridge objective experiment. It is a release-readiness pointer for reviewers, not a new experiment and not a product behavior change.

## Current Artifact

- Labeled outcome artifact: `docs/audit/manual-review/bridge_objective_labeled_outcome_rank-ee2ba6c816_rank-60910a47b4.json`
- Companion markdown: `docs/audit/manual-review/bridge_objective_labeled_outcome_rank-ee2ba6c816_rank-60910a47b4.md`
- Baseline run id: `rank-ee2ba6c816`
- Objective experiment run id: `rank-60910a47b4`
- Corpus snapshot: `source-snapshot-v2-candidate-plan-20260428`
- Embedding version: `v2-title-abstract-1536-cleantext-r1`
- Cluster version: `kmeans-l2-v2-cleantext-r1-k12`
- Baseline eligibility mode: `top50_cross_cluster_gte_0_40`
- Objective eligibility mode: `top50_cross040_exclude_persistent_shared_v1`

## Results

- Distinctness result: eligible bridge vs emerging Jaccard changed from `0.212121` to `0.081081`.
- Labeled quality result: good/acceptable share stayed `0.95` to `0.95`.
- Bridge-like result: yes/partial share stayed `0.95` to `0.95`.
- Decision field: `recommend_persistent_overlap_exclusion_as_experimental_arm=true`.
- Decision field: `ready_for_default=false`.

## Interpretation

The current evidence supports treating persistent-overlap exclusion as an experimental bridge review arm for this corpus snapshot. It does not support changing defaults.

This is not validation of bridge ranking quality.

Single-reviewer, top-20, offline audit material only.

Persistent-overlap exclusion is corpus-snapshot-specific and must not become default without rederivation on the active snapshot.

`ready_for_default=false`

## Caveats

- The labels are from one reviewer and a short-head offline audit, not a user study.
- The experiment compares one baseline run and one objective run on one corpus snapshot.
- The observed distinctness improvement is a diagnostic result, not proof of better recommendations.
- The bridge family remains a preview/diagnostics surface unless a later evaluation plan justifies stronger product claims.
- Ranking, API behavior, and web defaults remain unchanged by this evidence summary.

## Deferred Validation Steps

- Rederive any persistent-overlap exclusion on the active corpus snapshot before considering default behavior.
- Pre-register a larger, multi-reviewer labeling protocol with disagreement handling.
- Compare against the active production baseline and document run metadata before any launch decision.
- Add user-facing or task-grounded evaluation only after offline audit criteria are stable.
- Keep `NEXT_PUBLIC_ENABLE_EXPERIMENTAL_BRIDGE_VIEW` disabled for normal/public runs until evidence and defaults are revisited.
