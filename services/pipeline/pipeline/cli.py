from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg

from pipeline.bootstrap_loader import database_url_from_env, load_resolved_policy_from_database, run_bootstrap_ingest
from pipeline.clustering_persistence import count_included_missing_cluster_assignment
from pipeline.clustering_run import execute_clustering_run
from pipeline.embedding_persistence import (
    count_included_works_for_snapshot,
    count_missing_embedding_candidates,
    latest_corpus_snapshot_version_with_works,
)
from pipeline.embedding_run import execute_embedding_run
from pipeline.ranking_run import (
    MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY,
    execute_ranking_run,
    validate_bridge_weight_for_bridge_family,
)
from pipeline.recommendation_review_worksheet import (
    WorksheetError,
    write_recommendation_review_worksheet,
)
from pipeline.recommendation_review_summary import (
    ReviewSummaryError,
    run_recommendation_review_summary,
)
from pipeline.recommendation_review_rollup import (
    ReviewRollupError,
    run_recommendation_review_rollup,
)
from pipeline.bridge_experiment_readiness import (
    BridgeExperimentReadinessError,
    run_bridge_experiment_readiness,
)
from pipeline.bridge_signal_diagnostics import (
    BridgeSignalDiagnosticsError,
    run_bridge_signal_diagnostics,
)
from pipeline.work_text_repair import run_work_text_repair_cli
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
        help="Cap missing works processed on this run (smoke tests only; omit for full snapshot coverage)",
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
    ranking_parser.add_argument(
        "--cluster-version",
        default=None,
        help="Optional succeeded clustering_runs.cluster_version for ML2-5a bridge_score column (must match snapshot + embedding-version)",
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
        "--bridge-weight-for-family-bridge",
        type=lambda s: validate_bridge_weight_for_bridge_family(float(s)),
        default=0.0,
        metavar="W",
        help=(
            "Bridge family only: weight on cluster-boundary bridge_score in final_score (ML2-5b). "
            f"Default 0.0 (ML2-5a). Range [0.0, {MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY}]."
        ),
    )
    ranking_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    cluster_parser = subparsers.add_parser(
        "cluster-works",
        help="Cluster embedded included works for a snapshot/version identity",
    )
    cluster_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label to cluster (must already exist in embeddings table)",
    )
    cluster_parser.add_argument(
        "--cluster-version",
        required=True,
        help="Cluster assignment artifact label written to clusters + clustering_runs",
    )
    cluster_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    cluster_parser.add_argument(
        "--cluster-count",
        type=int,
        default=12,
        help="Target number of clusters for kmeans-l2-v0 (default 12)",
    )
    cluster_parser.add_argument(
        "--max-iterations",
        type=int,
        default=20,
        help="Maximum kmeans iterations (default 20)",
    )
    cluster_parser.add_argument("--note", default=None, help="Optional run notes")
    cluster_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    repair_parser = subparsers.add_parser(
        "repair-works-text",
        help="Re-apply title/abstract text cleanup (mojibake, HTML entities) to included works in a snapshot",
    )
    repair_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    repair_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many rows would change without writing",
    )
    repair_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    cov_parser = subparsers.add_parser(
        "embedding-coverage",
        help="Report how many included works in a snapshot have rows in embeddings for a version",
    )
    cov_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label (same as embed-works and cluster-works)",
    )
    cov_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    cov_parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        help="Exit with code 1 if any included work is missing an embedding, or (with --cluster-version) any included work lacks a cluster row",
    )
    cov_parser.add_argument(
        "--cluster-version",
        default=None,
        help="Optional cluster artifact label: report included works missing a clusters row (after cluster-works)",
    )
    cov_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    worksheet_parser = subparsers.add_parser(
        "recommendation-review-worksheet",
        help="Write a CSV of top recommendations for one succeeded ranking run (manual review scaffold)",
    )
    worksheet_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Succeeded materialized ranking run id (required; no default or latest resolution)",
    )
    worksheet_parser.add_argument(
        "--family",
        required=True,
        choices=sorted(["emerging", "bridge", "undercited"]),
        help="Recommendation family column to export",
    )
    worksheet_parser.add_argument(
        "--limit",
        type=int,
        required=True,
        help="Max rows (ordered by final_score desc, work_id asc)",
    )
    worksheet_parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path (e.g. docs/audit/manual-review/bridge_run.csv)",
    )
    worksheet_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    summary_parser = subparsers.add_parser(
        "recommendation-review-summary",
        help="Validate and summarize a filled recommendation review worksheet CSV (human labels)",
    )
    summary_parser.add_argument(
        "--input",
        required=True,
        help="Path to a completed worksheet CSV (same columns as recommendation-review-worksheet)",
    )
    summary_parser.add_argument(
        "--output",
        required=True,
        help="Path to write JSON summary (e.g. docs/audit/manual-review/bridge_RUN_summary.json)",
    )
    summary_parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Write summary with is_complete=false when labels are blank/invalid; default is strict (exit 2)",
    )
    summary_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write a short human-readable Markdown summary",
    )
    rollup_parser = subparsers.add_parser(
        "recommendation-review-rollup",
        help="Combine completed family review summaries into one run-level evaluation artifact",
    )
    rollup_parser.add_argument(
        "--summary",
        action="append",
        required=True,
        help="Path to family summary JSON (repeat for each family)",
    )
    rollup_parser.add_argument(
        "--output",
        required=True,
        help="Path to write rollup JSON (e.g. docs/audit/manual-review/rank_x_rollup.json)",
    )
    rollup_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write rollup Markdown",
    )

    bridge_readiness_parser = subparsers.add_parser(
        "bridge-experiment-readiness",
        help="Join recommendation review rollup with paper_scores top-k overlap for bridge weight go/no-go",
    )
    bridge_readiness_parser.add_argument(
        "--rollup",
        required=True,
        help="Path to rank-level recommendation review rollup JSON",
    )
    bridge_readiness_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (must match rollup provenance and ranking_runs row)",
    )
    bridge_readiness_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_readiness_parser.add_argument(
        "--output",
        required=True,
        help="Path to write readiness JSON",
    )
    bridge_readiness_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write readiness Markdown",
    )
    bridge_readiness_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    bridge_diag_parser = subparsers.add_parser(
        "bridge-signal-diagnostics",
        help="Read-only bridge signal diagnostics for one explicit ranking_run_id (paper_scores + ranking_runs)",
    )
    bridge_diag_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (no latest fallback)",
    )
    bridge_diag_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_diag_parser.add_argument(
        "--output",
        required=True,
        help="Path to write diagnostics JSON",
    )
    bridge_diag_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write diagnostics Markdown",
    )
    bridge_diag_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    args = parser.parse_args()

    if args.command == "recommendation-review-worksheet":
        if args.limit < 1 or args.limit > 200:
            parser.error("--limit must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        try:
            write_recommendation_review_worksheet(
                output_path=Path(args.output),
                database_url=args.database_url,
                ranking_run_id=rrid,
                family=args.family,
                limit=int(args.limit),
            )
        except WorksheetError as e:
            print(f"recommendation-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        return

    if args.command == "recommendation-review-summary":
        try:
            run_recommendation_review_summary(
                input_path=Path(args.input),
                output_path=Path(args.output),
                allow_incomplete=bool(args.allow_incomplete),
                markdown_path=Path(args.markdown_output)
                if args.markdown_output
                else None,
            )
        except ReviewSummaryError as e:
            print(f"recommendation-review-summary: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "recommendation-review-rollup":
        try:
            run_recommendation_review_rollup(
                summary_paths=[Path(x) for x in args.summary],
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output)
                if args.markdown_output
                else None,
            )
        except ReviewRollupError as e:
            print(f"recommendation-review-rollup: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "bridge-experiment-readiness":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        try:
            run_bridge_experiment_readiness(
                rollup_path=Path(args.rollup),
                ranking_run_id=rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeExperimentReadinessError as e:
            print(f"bridge-experiment-readiness: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "bridge-signal-diagnostics":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        try:
            run_bridge_signal_diagnostics(
                ranking_run_id=rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeSignalDiagnosticsError as e:
            print(f"bridge-signal-diagnostics: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

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
            cluster_version=args.cluster_version,
            bridge_weight_for_bridge_family=args.bridge_weight_for_family_bridge,
            note=args.note,
            low_cite_min_year=args.low_cite_min_year,
            low_cite_max_citations=args.low_cite_max_citations,
        )
        print(finalized.ranking_run_id)
        print(finalized.corpus_snapshot_version)
        return

    if args.command == "cluster-works":
        finalized = execute_clustering_run(
            database_url=args.database_url,
            cluster_version=args.cluster_version,
            embedding_version=args.embedding_version,
            corpus_snapshot_version=args.corpus_snapshot_version,
            cluster_count=args.cluster_count,
            max_iterations=args.max_iterations,
            note=args.note,
        )
        lines = [
            f"cluster_version={finalized.cluster_version}",
            f"embedding_version={finalized.embedding_version}",
            f"corpus_snapshot_version={finalized.corpus_snapshot_version}",
            f"algorithm={finalized.algorithm}",
            f"status={finalized.status}",
            f"total_input_works={finalized.counts.total_input_works}",
            f"clustered_works={finalized.counts.clustered_works}",
            f"cluster_count={finalized.counts.cluster_count}",
        ]
        print("\n".join(lines), file=sys.stderr)
        print(finalized.cluster_version)
        print(finalized.corpus_snapshot_version)
        return

    if args.command == "repair-works-text":
        snap, scanned, updated = run_work_text_repair_cli(
            database_url=args.database_url,
            corpus_snapshot_version=args.corpus_snapshot_version,
            dry_run=args.dry_run,
        )
        mode = "dry-run" if args.dry_run else "committed"
        print(
            f"repair-works-text ({mode}): corpus_snapshot_version={snap} "
            f"scanned={scanned} rows_changed={updated}",
            file=sys.stderr,
        )
        print(snap)
        print(updated)
        return

    if args.command == "embedding-coverage":
        dsn = args.database_url or database_url_from_env()
        with psycopg.connect(dsn) as conn:
            snap = args.corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
            if snap is None:
                parser.error("No corpus snapshot with included works found.")
            total = count_included_works_for_snapshot(conn, snap)
            missing = count_missing_embedding_candidates(
                conn,
                corpus_snapshot_version=snap,
                embedding_version=args.embedding_version,
            )
            missing_cluster: int | None = None
            if args.cluster_version:
                cr = conn.execute(
                    """
                    SELECT embedding_version, status
                    FROM clustering_runs
                    WHERE cluster_version = %s
                      AND corpus_snapshot_version = %s
                    """,
                    (args.cluster_version, snap),
                ).fetchone()
                if cr is None:
                    print(
                        "embedding-coverage: error: no clustering_runs row for "
                        f"cluster_version={args.cluster_version!r} and "
                        f"corpus_snapshot_version={snap!r}.",
                        file=sys.stderr,
                    )
                    sys.exit(2)
                run_emb, run_status = str(cr[0]), str(cr[1])
                if run_emb != args.embedding_version:
                    print(
                        "embedding-coverage: warning: clustering_runs.embedding_version="
                        f"{run_emb!r} differs from --embedding-version={args.embedding_version!r}.",
                        file=sys.stderr,
                    )
                if run_status != "succeeded":
                    print(
                        "embedding-coverage: warning: clustering_runs.status="
                        f"{run_status!r} (expected succeeded).",
                        file=sys.stderr,
                    )
                missing_cluster = count_included_missing_cluster_assignment(
                    conn,
                    corpus_snapshot_version=snap,
                    cluster_version=args.cluster_version,
                )
        embedded = total - missing
        lines = [
            f"corpus_snapshot_version={snap}",
            f"embedding_version={args.embedding_version}",
            f"included_works={total}",
            f"with_embedding={embedded}",
            f"missing_embedding={missing}",
        ]
        if args.cluster_version and missing_cluster is not None:
            lines.extend(
                [
                    f"cluster_version={args.cluster_version}",
                    f"with_cluster_assignment={total - missing_cluster}",
                    f"missing_cluster_assignment={missing_cluster}",
                ]
            )
        print("\n".join(lines), file=sys.stderr)
        print(snap)
        print(missing)
        gap = missing > 0 or (
            args.cluster_version is not None and missing_cluster is not None and missing_cluster > 0
        )
        if args.fail_on_gaps and gap:
            sys.exit(1)
        return


if __name__ == "__main__":
    main()
