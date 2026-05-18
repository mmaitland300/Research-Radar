# Fresh Product-Candidate Source Build (ml-fresh-product-candidate-source-build-v1)

## Executive Summary

This artifact freezes a larger product-candidate source in artifact-only mode when existing local candidate pools can be safely broadened. It does not create rankings, write databases, score hybrids, train, import labels, or authorize shadow/production.

- **Mode:** `artifact_only_freeze`
- **Status:** `blocked_needs_corpus_or_candidate_expansion`
- **Confirmatory eligible works:** 44
- **Candidate threshold met:** False
- **Recommended next stage:** `blocked_expand_corpus_or_candidate_generation`
- **Shadow scoring allowed:** False
- **Production default allowed:** False

## Candidate Count And SHA

- Candidate source ID: `artifact-union-emerging-1a62e9802e85`
- Candidate work-set SHA: `1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926`
- Canonical work IDs SHA: `1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926`
- Candidate rows frozen: 59
- Corpus snapshot version: `multi_snapshot_union`

## Old 217 Exclusion

- Old surface overlap count: 15
- Old 217 overlaps are excluded from confirmatory denominators.

## Underpowered 44 Overlap

- Underpowered source overlap count: 44
- Incremental works outside underpowered source: 0
- Threshold basis: confirmatory_eligible_work_count >= 100 and incremental works outside rank-3904fec89d >= candidate_gap 56

## Label Snapshot

- Labeled works: 20
- Label coverage rate: 0.4545
- Positive labeled works: 20
- Negative labeled works: 0
- Distinct negative works: 0
- Label thresholds currently met: False

## Threshold Checks

| Threshold | Observed | Required | Passed |
| --- | ---: | ---: | --- |
| `minimum_confirmatory_candidate_work_count` | 44 | 100 | False |
| `minimum_confirmatory_labeled_work_count` | 20 | 100 | False |
| `minimum_confirmatory_label_coverage_rate` | 0.4545 | 0.6000 | False |
| `minimum_confirmatory_positive_work_count` | 20 | 50 | False |
| `minimum_confirmatory_negative_work_count` | 0 | 20 | False |
| `minimum_distinct_negative_work_count` | 0 | 20 | False |

## Materializer Handoff

materializer extension required: ml-fresh-eval-surface-hybrid-materialize does not yet accept ml-fresh-product-candidate-source-build artifact inputs; do not claim materialization complete.

## Not Shadow / Not Production

- Artifact-only candidate source build; no production ranking/default changes.
- Read-only local Postgres SELECTs in v1; no database writes are performed.
- No hybrid scoring, training, embeddings, label import, API/web, shadow, or production changes.
- Old 217-work surface overlaps are excluded from confirmatory denominators.
- Underpowered 44-work source overlaps are tagged and separated from incremental expansion counts.
- Label readiness is informational only and does not authorize hybrid validation.
- No shadow or production authorization.
