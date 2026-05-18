# Fresh Product-Candidate Ranking Source (ml-fresh-product-candidate-ranking-source-v1)

## Executive Summary

This artifact freezes an existing read-only product-candidate ranking source, if one is large enough after excluding the old 217-work surface. It does not create rankings, score hybrids, train, import labels, or authorize shadow/production.

- **Status:** `blocked_no_source_meets_candidate_threshold`
- **Minimum confirmatory candidate works:** 100
- **Recommended next stage:** `create_new_or_larger_candidate_snapshot`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Why This Step Exists

The current fresh surface has only 44 confirmatory-eligible works, so labeling alone cannot satisfy the 100-work policy floor. This step looks for a larger already-existing source before any new product/corpus work is attempted.

## Sources Considered

| Ranking run | Snapshot | Works | Old overlap | Confirmatory eligible | Labeled | Negatives | Candidate pass |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `rank-3904fec89d` | `source-snapshot-20260425-044015` | 59 | 15 | 44 | 20 | 0 | False |
| `rank-38a09c7368` | `source-snapshot-20260328-170751` | 51 | 14 | 37 | 20 | 0 | False |
| `rank-83976f1097` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-808f9d7f4d` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-b39d9e0d4f` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-cf04ae30c6` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-16c1cfb490` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-17658d0f74` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-c34fa85261` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-c765e2de5c` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-63710a0277` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-19a2c8671f` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-d18414d7e7` | `source-snapshot-20260329-170012` | 38 | 14 | 24 | 13 | 0 | False |
| `rank-60910a47b4` | `source-snapshot-v2-candidate-plan-20260428` | 217 | 217 | 0 | 0 | 0 | False |
| `rank-9a02c81d40` | `source-snapshot-v2-candidate-plan-20260428` | 217 | 217 | 0 | 0 | 0 | False |
| `rank-bc1123e00c` | `source-snapshot-v2-candidate-plan-20260428` | 217 | 217 | 0 | 0 | 0 | False |
| `rank-ee2ba6c816` | `source-snapshot-v2-candidate-plan-20260428` | 217 | 217 | 0 | 0 | 0 | False |
| `rank-ed3f090ad7` | `source-snapshot-v2-candidate-plan-20260428` | 217 | 217 | 0 | 0 | 0 | False |

## Selected Source Or Blocker

No existing source met the minimum confirmatory candidate-work threshold after old-surface exclusion.

## Not Validation / Not Shadow / Not Production

- Read-only source discovery and freeze only.
- No ranking run is created and no database writes are performed.
- No hybrid scoring, training, embeddings, or label import.
- Old 217-work surface overlaps are excluded from confirmatory denominators.
- Label readiness is a snapshot from ml-label-dataset-v8, not new labeling.
- No shadow or production authorization.
