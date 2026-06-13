# Pinned model artifacts

This directory is intentionally small. It keeps only artifacts that deployed
scorer paths load directly or validate by hash. The API image copies each file
listed below; see `apps/api/Dockerfile`.

| artifact | why it remains on main | runtime or hash-pin reference |
| --- | --- | --- |
| `ml-offline-audit-embedding-scorer-v2.json` | Frozen Emerging scorer coefficients loaded at serving time. | `services/pipeline/pipeline/scorer_serving_io.py` loads `FROZEN_AUDIT_EMBEDDING_SCORER_PATH`. |
| `ml-bridge-rank-pct-hybrid-serving-plan-v1.json` | Bridge rollout plan that selects the bounded rank-percentile scorer and its inputs. | `services/pipeline/pipeline/ml_bridge_scorer_rollout_serving.py` loads `DEFAULT_SERVING_PLAN_PATH`. |
| `ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json` | Frozen Bridge scorer coefficients used by the serving plan. | The Bridge serving plan pins this file by SHA-256 and `ml_bridge_scorer_rollout_serving.py` validates it before loading. |
| `ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json` | Embedding provenance for the frozen Bridge scorer. | The Bridge serving plan pins this file by SHA-256 and `ml_bridge_scorer_rollout_serving.py` validates it before scoring. |

Do not edit these files in place. Export a new artifact version and update the
serving plan instead.

Historical labels, offline eval reports, rollout notes, and other non-runtime
audit JSON are working records, not product assets on `main`. The archived
experiment record lives on the `archive/ml-governance-audit` branch and in git
history before this directory was slimmed down.
