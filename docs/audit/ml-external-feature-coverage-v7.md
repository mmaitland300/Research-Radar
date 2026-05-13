# External Near-Miss Feature Coverage

Read-only coverage diagnostic for `ml_external_near_miss_audit` rows. This is featureization readiness only: no model training, no ranking run, no production behavior change.

## Provenance

- **label_dataset:** `docs/audit/ml-label-dataset-v7.json`
- **label_dataset_sha256:** `094af1a6083561803c26611e1d6f0afebba6eedec0d2e9ac21008f415117dc85`
- **label_dataset_version:** `ml-label-dataset-v7`
- **embedding_version:** `v2-title-abstract-1536-cleantext-r1`
- **corpus_snapshot_version_for_embed_eligibility:** `source-snapshot-v2-candidate-plan-20260428`
- **representative_paper_scores_run:** `rank-ee2ba6c816`
- **context_sidecar:** `docs/audit/manual-review/ml_external_near_miss_review_v1_context.json`
- **context_sidecar_sha256:** `e0cfa2368b24c85eb309106863a99527879182f6d9708ca7b4e69beb6aaf9261`
- **sidecar row_id parity:** `true`

## Coverage Summary

- **external rows:** `60`
- **unique OpenAlex work tokens:** `60`
- **works rows present:** `0`
- **embedding rows present:** `0`
- **embedding rows missing:** `60`
- **corpus_v2_embed_eligible:** `0`
- **representative paper_scores present:** `0`
- **sufficient text heuristic:** `56`

## Repo-Accurate Nuance

The current `corpus_v2_embed` / `embedding_persistence` candidate query is tied to `works.inclusion_status = 'included'` and a specific `corpus_snapshot_version`. External near-miss rows were sampled outside the committed snapshot manifest, so `embedding_row_present` and `corpus_v2_embed_eligible` are reported separately.

## Feature Inventory

Available without DB:

- `sample_reason`: `60` rows
- `cluster_id`: `60` rows
- `topics`: `60` rows
- `year`: `60` rows
- `citation_count`: `60` rows
- `openalex_identifiers`: `60` rows
- `source_metadata`: `60` rows
- `hidden_diagnostics`: `60` rows

Requires DB/materialization:

- `works_row`: `0` rows
- `db_full_abstract`: `0` rows
- `embedding_row`: `0` rows
- `representative_paper_scores`: `0` rows

Ranking-shaped features such as `paper_scores`, `final_score`, family scores, semantic scores, and cluster ranks require ranking materialization. External v7 rows have `ranking_run_id=null`, so those channels are unavailable unless the works are explicitly ranked later.

## Recommended Offline Feature Set

Future cross-pool text-first baseline: title+abstract embedding using DB full text when present, otherwise offline embedding from exported preview/context text with caveats. Add categorical metadata (`sample_reason`, topics/source), and keep `review_pool_variant` for stratification rather than treating it as a label.

## Caveats

- Not validation.
- Feature coverage only.
- Preview text is not a full abstract.
- Embedding presence is not a production ranking signal.
- The sufficient_text_for_embedding_heuristic flag is operational coverage only, not a quality label.
- The external pool is audit-only and not family-selected.
