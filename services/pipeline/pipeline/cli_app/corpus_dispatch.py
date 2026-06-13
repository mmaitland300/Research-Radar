from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_corpus_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

    if args.command == "corpus-expansion-preview":
        from pipeline.corpus_expansion_preview import run_corpus_expansion_preview_from_cli

        if args.per_bucket_sample < 10 or args.per_bucket_sample > 25:
            parser.error("--per-bucket-sample must be between 10 and 25")
        run_corpus_expansion_preview_from_cli(
            output=Path(args.output),
            markdown_output=Path(args.markdown_output),
            mailto=(args.mailto or "").strip(),
            per_bucket_sample=int(args.per_bucket_sample),
            mock_openalex=bool(args.mock_openalex),
        )
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return True

    if args.command == "corpus-v2-candidate-plan":
        from pipeline.corpus_v2_candidate_plan import run_corpus_v2_candidate_plan_from_cli

        if args.target_max < args.target_min:
            parser.error("--target-max must be >= --target-min")
        if args.per_bucket_limit < 1 or args.per_bucket_limit > 500:
            parser.error("--per-bucket-limit must be between 1 and 500")
        run_corpus_v2_candidate_plan_from_cli(
            output=Path(args.output),
            markdown_output=Path(args.markdown_output),
            mailto=(args.mailto or "").strip(),
            per_bucket_limit=int(args.per_bucket_limit),
            target_min=int(args.target_min),
            target_max=int(args.target_max),
            mock_openalex=bool(args.mock_openalex),
        )
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return True

    if args.command == "ismir-ingest-preview":
        from pipeline.ismir_ingest_preview import run_ismir_ingest_preview_from_cli

        if args.target_max < args.target_min:
            parser.error("--target-max must be >= --target-min")
        if args.max_works_per_bucket < 1 or args.max_works_per_bucket > 2000:
            parser.error("--max-works-per-bucket must be between 1 and 2000")
        run_ismir_ingest_preview_from_cli(
            output=Path(args.output),
            markdown_output=Path(args.markdown_output),
            mailto=(args.mailto or "").strip(),
            max_works_per_bucket=int(args.max_works_per_bucket),
            target_min=int(args.target_min),
            target_max=int(args.target_max),
            mock_openalex=bool(args.mock_openalex),
        )
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return True

    if args.command == "corpus-v2-ingest-from-plan":
        from pipeline.corpus_v2_ingest_from_plan import CorpusV2IngestError, run_corpus_v2_ingest_from_plan

        try:
            summary = run_corpus_v2_ingest_from_plan(
                candidate_plan_path=Path(args.candidate_plan),
                snapshot_version=args.snapshot_version,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
            )
        except CorpusV2IngestError as e:
            print(f"corpus-v2-ingest-from-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(summary["snapshot_version"])
        print(summary["ingest_run_id"])
        return True

    if args.command == "corpus-v2-hydrate-openalex":
        from pipeline.corpus_v2_hydrate_openalex import CorpusV2HydrateError, run_corpus_v2_hydrate_openalex

        try:
            summary = run_corpus_v2_hydrate_openalex(
                snapshot_version=args.snapshot_version,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
                mock_openalex=bool(args.mock_openalex),
            )
        except CorpusV2HydrateError as e:
            print(f"corpus-v2-hydrate-openalex: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(summary["snapshot_version"])
        print(summary["hydration_run_id"])
        return True

    if args.command == "ml-fresh-hybrid-snapshot-hydrate":
        from pipeline.ml_fresh_hybrid_snapshot_hydration import (
            MLFreshHybridSnapshotHydrationError,
            write_ml_fresh_hybrid_snapshot_hydration,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_fresh_hybrid_snapshot_hydration(
                fresh_hybrid_candidate_plan_ingest_path=Path(args.fresh_hybrid_candidate_plan_ingest),
                fresh_hybrid_corpus_candidate_plan_path=Path(args.fresh_hybrid_corpus_candidate_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                database_url=args.database_url,
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
                dry_run=bool(args.dry_run),
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                hydration_version=str(args.hydration_version),
                repo_root=repo_root,
            )
        except MLFreshHybridSnapshotHydrationError as e:
            print(f"ml-fresh-hybrid-snapshot-hydrate: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        _print_artifact_values(
            Path(args.output),
            ("metadata", "snapshot_version"),
            ("hydration_result", "recommended_next_stage"),
        )
        return True

    if args.command == "ml-shadow-scorer-second-snapshot-hydration":
        from pipeline.ml_shadow_scorer_second_snapshot_hydration import (
            MLShadowScorerSecondSnapshotHydrationError,
            write_ml_shadow_scorer_second_snapshot_hydration,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_shadow_scorer_second_snapshot_hydration(
                second_candidate_plan_ingest_path=Path(args.second_candidate_plan_ingest),
                second_hybrid_candidate_plan_path=Path(args.second_hybrid_candidate_plan),
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                database_url=args.database_url,
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
                dry_run=bool(args.dry_run),
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                hydration_version=str(args.hydration_version),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondSnapshotHydrationError as e:
            print(f"ml-shadow-scorer-second-snapshot-hydration: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        _print_artifact_values(
            Path(args.output),
            ("metadata", "snapshot_version"),
            ("hydration_result", "snapshot_embedding_ready"),
            ("recommended_next_stage",),
        )
        return True

    if args.command == "corpus-v2-embed":
        from pipeline.corpus_v2_embed import CorpusV2EmbedError, run_corpus_v2_embed

        try:
            summary = run_corpus_v2_embed(
                snapshot_version=args.snapshot_version,
                embedding_version=args.embedding_version,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
                model=args.model,
                batch_size=int(args.batch_size),
                replace=bool(args.replace),
            )
        except CorpusV2EmbedError as e:
            print(f"corpus-v2-embed: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(summary["snapshot_version"])
        print(summary["embedding_version"])
        print(summary["embedded_count"])
        return True

    if args.command == "corpus-v2-compose-snapshot":
        from pipeline.snapshot_membership import SnapshotMembershipError, run_compose_snapshot_from_cli

        try:
            summary = run_compose_snapshot_from_cli(
                snapshot_version=args.snapshot_version,
                from_snapshots=list(args.from_snapshots or []),
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
                note=args.note,
            )
        except SnapshotMembershipError as e:
            print(f"corpus-v2-compose-snapshot: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(summary["snapshot_version"])
        print(summary["membership_count"])
        return True

    if args.command == "ml-fresh-hybrid-snapshot-embed":
        from pipeline.ml_fresh_hybrid_snapshot_embeddings import (
            MLFreshHybridSnapshotEmbeddingsError,
            write_ml_fresh_hybrid_snapshot_embeddings,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_fresh_hybrid_snapshot_embeddings(
                fresh_hybrid_snapshot_hydration_path=Path(args.fresh_hybrid_snapshot_hydration),
                fresh_hybrid_candidate_plan_ingest_path=Path(args.fresh_hybrid_candidate_plan_ingest),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                embedding_version=str(args.embedding_version),
                database_url=args.database_url,
                mock_embeddings=bool(args.mock_embeddings),
                dry_run=bool(args.dry_run),
                limit=args.limit,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLFreshHybridSnapshotEmbeddingsError as e:
            print(f"ml-fresh-hybrid-snapshot-embed: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        _print_artifact_values(
            Path(args.output),
            ("metadata", "snapshot_version"),
            ("metadata", "embedding_version"),
            ("embedding_result", "recommended_next_stage"),
        )
        return True

    if args.command == "ml-shadow-scorer-second-snapshot-embeddings":
        from pipeline.ml_shadow_scorer_second_snapshot_embeddings import (
            MLShadowScorerSecondSnapshotEmbeddingsError,
            write_ml_shadow_scorer_second_snapshot_embeddings,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_shadow_scorer_second_snapshot_embeddings(
                second_snapshot_hydration_path=Path(args.second_snapshot_hydration),
                second_candidate_plan_ingest_path=Path(args.second_candidate_plan_ingest),
                second_hybrid_candidate_plan_path=Path(args.second_hybrid_candidate_plan),
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                embedding_version=str(args.embedding_version),
                database_url=args.database_url,
                mock_embeddings=bool(args.mock_embeddings),
                dry_run=bool(args.dry_run),
                limit=args.limit,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondSnapshotEmbeddingsError as e:
            print(f"ml-shadow-scorer-second-snapshot-embeddings: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        _print_artifact_values(
            Path(args.output),
            ("metadata", "snapshot_version"),
            ("metadata", "embedding_version"),
            ("embedding_result", "recommended_next_stage"),
        )
        return True

    if args.command == "ml-shadow-scorer-second-product-candidate-ranking":
        from pipeline.ml_shadow_scorer_second_product_candidate_ranking import (
            MLShadowScorerSecondProductCandidateRankingError,
            write_ml_shadow_scorer_second_product_candidate_ranking,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_shadow_scorer_second_product_candidate_ranking(
                second_snapshot_embeddings_path=Path(args.second_snapshot_embeddings),
                second_snapshot_hydration_path=Path(args.second_snapshot_hydration),
                second_candidate_plan_ingest_path=Path(args.second_candidate_plan_ingest),
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                embedding_version=str(args.embedding_version),
                ranking_version=str(args.ranking_version),
                family=str(args.family),
                database_url=args.database_url,
                dry_run=bool(args.dry_run),
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondProductCandidateRankingError as e:
            print(f"ml-shadow-scorer-second-product-candidate-ranking: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        _print_artifact_values(
            Path(args.output),
            ("ranking_result", "ranking_run_id"),
            ("ranking_result", "paper_scores_written_count"),
            ("ranking_result", "recommended_next_stage"),
        )
        return True

    if args.command == "ml-fresh-hybrid-product-candidate-ranking":
        from pipeline.ml_fresh_hybrid_product_candidate_ranking import (
            MLFreshHybridProductCandidateRankingError,
            write_ml_fresh_hybrid_product_candidate_ranking,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_fresh_hybrid_product_candidate_ranking(
                fresh_hybrid_snapshot_embeddings_path=Path(args.fresh_hybrid_snapshot_embeddings),
                fresh_hybrid_snapshot_hydration_path=Path(args.fresh_hybrid_snapshot_hydration),
                fresh_hybrid_candidate_plan_ingest_path=Path(args.fresh_hybrid_candidate_plan_ingest),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                embedding_version=str(args.embedding_version),
                ranking_version=str(args.ranking_version),
                family=str(args.family),
                database_url=args.database_url,
                dry_run=bool(args.dry_run),
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLFreshHybridProductCandidateRankingError as e:
            print(f"ml-fresh-hybrid-product-candidate-ranking: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        _print_artifact_values(
            Path(args.output),
            ("ranking_result", "ranking_run_id"),
            ("ranking_result", "paper_scores_written_count"),
            ("ranking_result", "recommended_next_stage"),
        )
        return True

    if args.command == "cluster-inspection":
        try:
            payload = run_cluster_inspection(
                corpus_snapshot_version=args.corpus_snapshot_version,
                embedding_version=args.embedding_version,
                cluster_version=args.cluster_version,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
            )
        except ClusterInspectionError as e:
            print(f"cluster-inspection: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(payload["provenance"]["corpus_snapshot_version"])
        print(payload["provenance"]["cluster_version"])
        return True

    return False
