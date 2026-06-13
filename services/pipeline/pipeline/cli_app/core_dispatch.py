from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_core_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

    policy = CorpusPolicy()

    if args.command == "print-policy":
        if args.json:
            import json

            print(json.dumps(policy.as_dict(), indent=2))
        else:
            print(policy)
        return True

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
        return True

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
        return True

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
        return True

    if args.command == "ranking-run":
        finalized = execute_ranking_run(
            database_url=args.database_url,
            ranking_version=args.ranking_version,
            corpus_snapshot_version=args.corpus_snapshot_version,
            embedding_version=args.embedding_version,
            cluster_version=args.cluster_version,
            bridge_weight_for_bridge_family=args.bridge_weight_for_family_bridge,
            bridge_eligibility_mode=args.bridge_eligibility_mode,
            note=args.note,
            low_cite_min_year=args.low_cite_min_year,
            low_cite_max_citations=args.low_cite_max_citations,
        )
        print(finalized.ranking_run_id)
        print(finalized.corpus_snapshot_version)
        return True

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
        return True

    if args.command == "repair-works-text":
        snap, scanned, updated = compat.run_work_text_repair_cli(
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
        return True

    if args.command == "embedding-coverage":
        dsn = args.database_url or database_url_from_env()
        with psycopg_module.connect(dsn) as conn:
            snap = args.corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
            if snap is None:
                parser.error("No corpus snapshot with included works found.")
            total = compat.count_included_works_for_snapshot(conn, snap)
            missing = compat.count_missing_embedding_candidates(
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
                missing_cluster = compat.count_included_missing_cluster_assignment(
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
        return True

    return False
