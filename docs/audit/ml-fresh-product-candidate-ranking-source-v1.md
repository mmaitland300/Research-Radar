# Fresh Product-Candidate Ranking Source (ml-fresh-product-candidate-ranking-source-v1)

## Executive Summary

This artifact freezes an existing read-only product-candidate ranking source, if one is large enough after excluding the old 217-work surface. It does not create rankings, score hybrids, train, import labels, or authorize shadow/production.

- **Status:** `source_frozen_needs_materialization`
- **Minimum confirmatory candidate works:** 100
- **Recommended next stage:** `rerun_fresh_eval_surface_materialize_with_selected_source`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Why This Step Exists

The current fresh surface has only 44 confirmatory-eligible works, so labeling alone cannot satisfy the 100-work policy floor. This step looks for a larger already-existing source before any new product/corpus work is attempted.

## Sources Considered

| Ranking run | Snapshot | Works | Old overlap | Confirmatory eligible | Labeled | Negatives | Candidate pass |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `rank-9f4b2a2084` | `source-snapshot-fresh-hybrid-v1-20260518` | 358 | 215 | 143 | 1 | 0 | True |

## Selected Source Or Blocker

- Ranking run: `rank-9f4b2a2084`
- Family: `emerging`
- Snapshot: `source-snapshot-fresh-hybrid-v1-20260518`
- Candidate SHA: `927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6`
- Confirmatory eligible works: 143
- Old 217 overlap excluded: 215
- Labeled works: 1
- Label coverage rate: 0.0070
- Positive labeled works: 1
- Negative labeled works: 0

## Materializer Rerun Command

```powershell
py -m pipeline.cli ml-fresh-eval-surface-hybrid-materialize --fresh-surface-policy ../../docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json --label-dataset ../../docs/audit/ml-label-dataset-v8.json --conflict-policy ../../docs/audit/ml-label-conflict-policy.md --family emerging --ranking-run-id rank-9f4b2a2084 --output ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.json --markdown-output ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.md
```

## Not Validation / Not Shadow / Not Production

- Read-only source discovery and freeze only.
- No ranking run is created and no database writes are performed.
- No hybrid scoring, training, embeddings, or label import.
- Old 217-work surface overlaps are excluded from confirmatory denominators.
- Label readiness is a snapshot from ml-label-dataset-v8, not new labeling.
- No shadow or production authorization.
