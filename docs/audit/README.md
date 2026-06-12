# Pinned model artifacts

This directory contains only the frozen artifacts that the deployed scorers
load at runtime or that pin their provenance. The runtime files are copied
into the API Docker image (see `apps/api/Dockerfile`):

- `ml-offline-audit-embedding-scorer-v2.json` - frozen audit-embedding scorer
  used by the bounded Emerging scorer rollout
  (`pipeline/ml_scorer_rollout_serving.py`).
- `ml-bridge-rank-pct-hybrid-serving-plan-v1.json` - serving plan for the
  bounded Bridge rank-pct hybrid scorer
  (`pipeline/ml_bridge_scorer_rollout_serving.py`).
- `ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json` -
  frozen Bridge scorer coefficients referenced by the serving plan
  (SHA-256 pinned).
- `ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json` - embeddings
  provenance referenced by the serving plan (SHA-256 pinned).

The remaining `ml-*.json` files are the provenance chain for the deployed
`ml-shadow-scorer-v1` (spec, validation, audit outputs, policy, and label
dataset pins); the scorer modules and their tests validate against them.

Do not edit these files in place; the serving plan and scorer audits validate
their hashes. Train and export a new artifact version instead.

The full experiment and evaluation record that used to live in this directory
(labeling worksheets, offline eval reports, rollout review notes) is preserved
on the `archive/ml-governance-audit` branch and in git history before this
directory was slimmed down.
