from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline.bootstrap_loader import database_url_from_env, load_resolved_policy_from_database, run_bootstrap_ingest
from pipeline.embedding_run import execute_embedding_run
from pipeline.ranking_run import execute_ranking_run
from pipeline.jobs import (
    create_bootstrap_bundle,
    write_bootstrap_plan,
    write_ingest_artifacts,
    write_source_resolution_manifest,
    write_source_resolution_results,
)
from pipeline.openalex import build_bootstrap_work_plans, build_source_resolution_plans
from pipeline.policy import CorpusPolicy, corpus_policy_with_openalex_source_ids
from pipeline.source_resolution import resolve_all_sources, slug_to_openalex_id_map


def main() -> None:
    parser = argparse.ArgumentParser(description="Research Radar pipeline utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    policy_parser = subparsers.add_parser("print-policy", help="Print the active corpus policy")
    policy_parser.add_argument("--json", action="store_true", help="Print as JSON")

    bootstrap_parser = subparsers.add_parser("bootstrap-plan", help="Write bootstrap snapshot, ingest run, and query plans")
    bootstrap_parser.add_argument("--output", default="artifacts", help="Output directory")
    bootstrap_parser.add_argument("--note", default="Bootstrap ingest planning run", help="Snapshot note")
    src = bootstrap_parser.add_mutually_exclusive_group(required=False)
    src.add_argument(
        "--resolve-openalex",
        action="store_true",
        help="Resolve canonical source IDs via OpenAlex /sources (required unless DB or coded IDs exist)",
    )
    src.add_argument(
        "--database-source-ids",
        action="store_true",
        help="Load openalex_source_id from Postgres source_policies (after a prior bootstrap-run resolve)",
    )
    bootstrap_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL for --database-source-ids (default: DATABASE_URL or PG* env)",
    )
    bootstrap_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent when using --resolve-openalex",
    )

    run_parser = subparsers.add_parser(
        "bootstrap-run",
        help="Execute OpenAlex bootstrap: raw pages, Postgres load, manifest (needs DATABASE_URL or PG*)",
    )
    run_parser.add_argument("--output", default="artifacts", help="Manifest and snapshot metadata directory")
    run_parser.add_argument("--raw-root", default="artifacts", help="Root directory for raw OpenAlex page JSON")
    run_parser.add_argument("--note", default="API bootstrap ingest", help="Snapshot note")
    run_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL env or PGHOST/PGUSER/PGPASSWORD/PGDATABASE)",
    )
    run_parser.add_argument(
        "--max-pages-per-source",
        type=int,
        default=None,
        help="Cap pages per venue plan (for smoke tests; default: paginate until exhausted)",
    )
    run_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent (default: OPENALEX_MAILTO env)",
    )

    embed_parser = subparsers.add_parser(
        "embed-works",
        help="Write one embedding per included work from title + abstract",
    )
    embed_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label stored on embeddings rows (e.g. v1-title-abstract-1536)",
    )
    embed_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    embed_parser.add_argument(
        "--model",
        default="text-embedding-3-small",
        help="Embedding model label for the provider request (default: text-embedding-3-small)",
    )
    embed_parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Texts per embedding request batch",
    )
    embed_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap missing works processed on this run (useful for smoke tests)",
    )
    embed_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    ranking_parser = subparsers.add_parser(
        "ranking-run",
        help="Create ranking_runs row, write stub paper_scores, finalize (Step 2 plumbing)",
    )
    ranking_parser.add_argument(
        "--ranking-version",
        required=True,
        help="Algorithm / config label (e.g. v0-heuristic-no-embeddings)",
    )
    ranking_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    ranking_parser.add_argument(
        "--embedding-version",
        default="none-v0",
        help="Embedding artifact version label stored on the run",
    )
    ranking_parser.add_argument("--note", default=None, help="Optional run notes")
    ranking_parser.add_argument(
        "--low-cite-min-year",
        type=int,
        default=2019,
        help="Undercited family: min publication year (default 2019; see docs/candidate-pool-low-cite.md)",
    )
    ranking_parser.add_argument(
        "--low-cite-max-citations",
        type=int,
        default=30,
        help="Undercited family: max citation_count inclusive (default 30)",
    )
    ranking_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    args = parser.parse_args()
    policy = CorpusPolicy()

    if args.command == "print-policy":
        if args.json:
            import json

            print(json.dumps(policy.as_dict(), indent=2))
        else:
            print(policy)
        return

    if args.command == "bootstrap-plan":
        output_dir = Path(args.output)
        outcomes = None
        policy_model = policy
        if args.resolve_openalex:
            outcomes = resolve_all_sources(policy, mailto=args.mailto)
            policy_model = corpus_policy_with_openalex_source_ids(policy, slug_to_openalex_id_map(outcomes))
        elif args.database_source_ids:
            dsn = args.database_url or database_url_from_env()
            policy_model = load_resolved_policy_from_database(dsn, policy)
        elif any(not s.openalex_source_id for s in policy.source_policies):
            parser.error(
                "bootstrap-plan needs canonical OpenAlex source ids: use --resolve-openalex, "
                "--database-source-ids, or set openalex_source_id on each SourcePolicy in policy.py"
            )

        snapshot, ingest_run = create_bootstrap_bundle(policy=policy_model, note=args.note)
        write_ingest_artifacts(output_dir, snapshot, ingest_run)
        write_source_resolution_manifest(output_dir, snapshot, build_source_resolution_plans(policy))
        if outcomes is not None:
            write_source_resolution_results(output_dir, snapshot, outcomes)
        write_bootstrap_plan(output_dir, snapshot, build_bootstrap_work_plans(policy_model))
        print(snapshot.source_snapshot_version)
        print(ingest_run.ingest_run_id)
        return

    if args.command == "bootstrap-run":
        output_dir = Path(args.output)
        raw_root = Path(args.raw_root)
        finalized = run_bootstrap_ingest(
            policy=policy,
            output_dir=output_dir,
            raw_root=raw_root,
            note=args.note,
            database_url=args.database_url,
            mailto=args.mailto,
            max_pages_per_source=args.max_pages_per_source,
        )
        print(finalized.ingest_run_id)
        print(finalized.source_snapshot_version)
        return

    if args.command == "embed-works":
        summary = execute_embedding_run(
            database_url=args.database_url,
            embedding_version=args.embedding_version,
            corpus_snapshot_version=args.corpus_snapshot_version,
            model=args.model,
            batch_size=args.batch_size,
            limit=args.limit,
        )
        lines = [
            f"embedding_version={summary.embedding_version}",
            f"corpus_snapshot_version={summary.corpus_snapshot_version}",
            f"model={summary.model}",
            f"total_included_works={summary.total_included_works}",
            f"already_embedded_before_run={summary.already_embedded_works}",
            f"missing_before_run={summary.missing_embedding_works}",
            f"candidate_works_this_run={summary.candidate_works}",
            f"planned_batches={summary.planned_batches}",
            f"batches_committed={summary.batch_count}",
            f"rows_written_this_run={summary.rows_written}",
            f"still_missing_after_run={summary.still_missing_after_run}",
        ]
        print("\n".join(lines), file=sys.stderr)
        print(summary.embedding_version)
        print(summary.corpus_snapshot_version)
        print(summary.rows_written)
        return

    if args.command == "ranking-run":
        finalized = execute_ranking_run(
            database_url=args.database_url,
            ranking_version=args.ranking_version,
            corpus_snapshot_version=args.corpus_snapshot_version,
            embedding_version=args.embedding_version,
            note=args.note,
            low_cite_min_year=args.low_cite_min_year,
            low_cite_max_citations=args.low_cite_max_citations,
        )
        print(finalized.ranking_run_id)
        print(finalized.corpus_snapshot_version)
        return


if __name__ == "__main__":
    main()
