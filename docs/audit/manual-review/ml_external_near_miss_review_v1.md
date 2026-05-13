# External near-miss review worksheet (`ml-external-near-miss-review-v1`)

## Purpose

Reviewer-blind worksheet for plausible but not-yet-curated external music/audio/recommender near misses. This expands negative-boundary label coverage outside the current 217-work snapshot; it is not model training, ranking, validation, or production readiness.

## Provenance

- **worksheet_version:** `ml-external-near-miss-review-v1`
- **review_pool_variant:** `ml_external_near_miss_audit`
- **sample_seed:** `20260514`
- **row_id formula:** `sha256(worksheet_version|sample_seed|paper_id)`
- **label_dataset:** `docs/audit/ml-label-dataset-v6.json`
- **label_dataset_sha256:** `bb7c5e786b6a9297ba70095a31168f2dd35596dcc2905a00ba8cb27667086f4b`
- **conflict_policy:** `docs/audit/ml-label-conflict-policy.md`
- **conflict_policy_sha256:** `d0591de1a2bd9ab75c64f2318e9a5ea0b7acd94902e083931049893416d47841`
- **corpus_snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **outside-217 exclusion source:** `candidate_plan_manifest` `docs/audit/corpus-v2-candidate-plan-20260428.json`
- **outside-217 exclusion count:** `217`
- **v6 labeled exclusion count:** `237`
- **seen-unlabeled v6 count:** `0`
- **csv_output:** `docs/audit/manual-review/ml_external_near_miss_review_v1.csv`
- **context_sidecar_output:** `docs/audit/manual-review/ml_external_near_miss_review_v1_context.json`
- **markdown_output:** `docs/audit/manual-review/ml_external_near_miss_review_v1.md`

## Reviewer CSV Policy

The reviewer CSV contains only reviewer-facing identity, bibliographic, topic, abstract-preview, sample_reason, and blank label columns. It excludes ranking identifiers, score/rank fields, family score/rank JSON, learned logits, model predictions, internal database IDs, and snapshot/embedding/cluster version fields.

`cluster_id` is the documented sentinel `ext` for external candidates because no pipeline k-means cluster assignment exists outside the snapshot.

## Acquisition Summary

- **requested rows:** `60`
- **achieved rows:** `60`
- **shortfall:** `0`
- **fallback rows:** `0`
- **raw OpenAlex candidates:** `650`
- **credible non-fallback candidate pool after filters:** `472`
- **pool supported requested near-miss intent:** `true`
- **selection note:** Requested row count was supported by non-fallback external near-miss strategies.

## Row Counts By Sample Reason

| sample_reason | rows |
|---|---:|
| `lexical_music_surface_match` | 10 |
| `adjacent_audio_not_mir` | 10 |
| `education_health_surface_match` | 10 |
| `industrial_bioacoustic_surface_match` | 10 |
| `recommender_not_music_specific` | 10 |
| `topic_neighbor_near_miss` | 10 |

## Candidate Counts By Strategy

| strategy | raw | after_filter | selected |
|---|---:|---:|---:|
| `lexical_music_surface_match` | 100 | 82 | 10 |
| `adjacent_audio_not_mir` | 100 | 76 | 10 |
| `education_health_surface_match` | 100 | 83 | 10 |
| `industrial_bioacoustic_surface_match` | 100 | 82 | 10 |
| `recommender_not_music_specific` | 100 | 87 | 10 |
| `topic_neighbor_near_miss` | 100 | 62 | 10 |
| `fallback_deterministic_fill` | 50 | 24 | 0 |

## Query Metadata

| strategy | returned | OpenAlex count | query |
|---|---:|---:|---|
| `lexical_music_surface_match` | 50 | 1406 | `music recommendation user behavior playlist platform` |
| `lexical_music_surface_match` | 50 | 2427 | `music information retrieval metadata discovery` |
| `adjacent_audio_not_mir` | 50 | 11506 | `audio representation learning environmental sound classification` |
| `adjacent_audio_not_mir` | 50 | 2877 | `speech audio foundation model acoustic scene classification` |
| `education_health_surface_match` | 50 | 4715 | `music therapy recommendation health patient audio` |
| `education_health_surface_match` | 50 | 16779 | `music education learning system recommender audio` |
| `industrial_bioacoustic_surface_match` | 50 | 1527 | `industrial machine fault diagnosis audio sound` |
| `industrial_bioacoustic_surface_match` | 50 | 1337 | `bioacoustic environmental sound detection deep learning` |
| `recommender_not_music_specific` | 50 | 41607 | `recommender system personalization platform user engagement` |
| `recommender_not_music_specific` | 50 | 45853 | `recommendation algorithm fairness user modeling platform` |
| `topic_neighbor_near_miss` | 50 | 2899 | `audio dataset bias benchmark machine listening` |
| `topic_neighbor_near_miss` | 50 | 2921 | `multimodal sound source recognition metadata` |
| `fallback_deterministic_fill` | 50 | 15502 | `sound recommendation learning classification audio` |

## Future Ingest Note

When labeled, a later dataset ingest should use a dated labeled copy such as `ml_external_near_miss_review_v1_labeled_YYYY-MM-DD.csv`, merge this sidecar by `row_id`, and keep `review_pool_variant=ml_external_near_miss_audit` distinct unless an experiment explicitly pools it.

## Caveats

- This worksheet is not validation.
- Rows are for reviewer-blind external near-miss manual labeling only.
- No model is trained, no ranking is run, and no production ranking change is supported.
- The reviewer CSV intentionally hides score, rank, model, ranking-run, and internal database fields.
