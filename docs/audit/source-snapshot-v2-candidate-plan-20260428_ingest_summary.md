# Corpus v2 source snapshot ingest summary

- **snapshot_version:** `source-snapshot-v2-candidate-plan-20260428`
- **ingest_run_id:** `ingest-2f76067014`
- **candidate_plan_sha256:** `5c3c78ba760b2e1a57ef1d3fc69242cdb30d03b5a32696167f0bb01297d94b66`
- **selected_total:** `217`
- **inserted_count:** `137`
- **updated_count:** `80`
- **skipped_existing_count:** `0`
- **failed_count:** `0`
- **missing_abstract_count:** `217`
- **missing_doi_count:** `9`
- **defaulted_language_count:** `217`
- **unknown_type_count:** `217`
- **embedding_ready_count:** `0`
- **embedding_blocked_count:** `217`
- **snapshot_embedding_ready:** `False`
- **openalex_enrichment:** `not_run`

## Embedding Readiness

This command creates a source snapshot import, not an embedding-ready corpus. Missing abstracts remain `NULL`; defaulted language values come from the candidate-plan policy; `unknown` work types are not validated document types.

## Counts by bucket

- **audio_ml_signal_processing:** `21`
- **core_mir_existing_sources:** `80`
- **ismir_proceedings_or_mir_conference:** `62`
- **music_recommender_systems:** `19`
- **source_separation_benchmarks:** `15`
- **symbolic_music_and_harmony:** `20`

## Warnings

- Candidate plan omits abstracts for one or more works; abstract remains NULL until text hydration.
- Candidate plan omits OpenAlex work type for one or more works; stored missing type as 'unknown' and did not validate it as an included document type.
- Candidate plan omits language for one or more works; stored missing language as 'en' from the candidate-plan policy filter, not observed OpenAlex metadata.
- No live OpenAlex enrichment, embeddings, clustering, ranking, or bridge-weight writes were run.

## Next step

metadata/text hydration for this snapshot, or an explicit title-only embedding version; do not run ranking yet
