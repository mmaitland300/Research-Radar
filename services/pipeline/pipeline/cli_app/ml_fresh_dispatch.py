from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_ml_fresh_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

    if args.command == "ml-fresh-eval-labeling-plan-hybrid":
        from pipeline.ml_fresh_eval_labeling_plan_hybrid import (
            MLFreshEvalLabelingPlanHybridError,
            write_ml_fresh_eval_labeling_plan_hybrid,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_eval_labeling_plan_hybrid(
                fresh_eval_surface_path=Path(args.fresh_eval_surface),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                repo_root=repo_root,
            )
        except MLFreshEvalLabelingPlanHybridError as e:
            print(f"ml-fresh-eval-labeling-plan-hybrid: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-fresh-eval-labeling-worksheet-hybrid":
        from pipeline.ml_fresh_eval_labeling_worksheet_hybrid import (
            MLFreshEvalLabelingWorksheetHybridError,
            write_ml_fresh_eval_labeling_worksheet_hybrid,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_csv = Path(args.output)
        out_context = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_fresh_eval_labeling_worksheet_hybrid(
                fresh_eval_surface_path=Path(args.fresh_eval_surface),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_csv,
                context_output_path=out_context,
                markdown_output_path=out_md,
                rows=int(args.rows),
                seed=int(args.seed),
                worksheet_version=str(args.worksheet_version),
                review_pool_variant=str(args.review_pool_variant),
                database_url=args.database_url,
                repo_root=repo_root,
            )
        except MLFreshEvalLabelingWorksheetHybridError as e:
            print(f"ml-fresh-eval-labeling-worksheet-hybrid: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_context.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(f"achieved_rows={payload['metadata']['achieved_rows']}", file=sys.stderr)
        return True

    if args.command == "ml-fresh-eval-positive-topup-worksheet-hybrid":
        from pipeline.ml_fresh_eval_positive_topup_worksheet_hybrid import (
            MLFreshEvalPositiveTopupWorksheetHybridError,
            write_ml_fresh_eval_positive_topup_worksheet_hybrid,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_csv = Path(args.output)
        out_context = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_fresh_eval_positive_topup_worksheet_hybrid(
                fresh_eval_surface_path=Path(args.fresh_eval_surface),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_csv,
                context_output_path=out_context,
                markdown_output_path=out_md,
                requested_rows=int(args.requested_rows),
                seed=int(args.seed),
                worksheet_version=str(args.worksheet_version),
                review_pool_variant=str(args.review_pool_variant),
                repo_root=repo_root,
            )
        except MLFreshEvalPositiveTopupWorksheetHybridError as e:
            print(f"ml-fresh-eval-positive-topup-worksheet-hybrid: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_context.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(f"achieved_rows={payload['metadata']['achieved_rows']}", file=sys.stderr)
        return True

    if args.command == "ml-fresh-candidate-source-expansion-plan":
        from pipeline.ml_fresh_candidate_source_expansion_plan import (
            MLFreshCandidateSourceExpansionPlanError,
            write_ml_fresh_candidate_source_expansion_plan,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_candidate_source_expansion_plan(
                fresh_product_candidate_ranking_source_path=Path(args.fresh_product_candidate_ranking_source),
                fresh_eval_labeling_plan_path=Path(args.fresh_eval_labeling_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                repo_root=repo_root,
            )
        except MLFreshCandidateSourceExpansionPlanError as e:
            print(f"ml-fresh-candidate-source-expansion-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-fresh-product-candidate-source-build":
        from pipeline.ml_fresh_product_candidate_source_build import (
            MLFreshProductCandidateSourceBuildError,
            write_ml_fresh_product_candidate_source_build,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_product_candidate_source_build(
                fresh_candidate_source_expansion_plan_path=Path(args.fresh_candidate_source_expansion_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                database_url=args.database_url,
                family=str(args.family),
                min_confirmatory_eligible_works=args.min_confirmatory_eligible_works,
                mode=str(args.mode),
                write_eval_db_source=bool(args.write_eval_db_source),
                build_version=str(args.build_version),
                repo_root=repo_root,
            )
        except MLFreshProductCandidateSourceBuildError as e:
            print(f"ml-fresh-product-candidate-source-build: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-fresh-hybrid-corpus-candidate-plan":
        from pipeline.ml_fresh_hybrid_corpus_candidate_plan import (
            MLFreshHybridCorpusCandidatePlanError,
            write_ml_fresh_hybrid_corpus_candidate_plan,
        )

        if args.target_max < args.target_min:
            parser.error("--target-max must be >= --target-min")
        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_hybrid_corpus_candidate_plan(
                fresh_product_candidate_source_build_path=Path(args.fresh_product_candidate_source_build),
                fresh_candidate_source_expansion_plan_path=Path(args.fresh_candidate_source_expansion_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                target_min=int(args.target_min),
                target_max=int(args.target_max),
                mailto=args.mailto,
                repo_root=repo_root,
            )
        except MLFreshHybridCorpusCandidatePlanError as e:
            print(f"ml-fresh-hybrid-corpus-candidate-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-fresh-hybrid-candidate-plan-ingest":
        from pipeline.ml_fresh_hybrid_candidate_plan_ingest import (
            MLFreshHybridCandidatePlanIngestError,
            write_ml_fresh_hybrid_candidate_plan_ingest,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_hybrid_candidate_plan_ingest(
                fresh_hybrid_corpus_candidate_plan_path=Path(args.fresh_hybrid_corpus_candidate_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                snapshot_version=args.snapshot_version,
                database_url=args.database_url,
                ingest_version=str(args.ingest_version),
                dry_run=bool(args.dry_run),
                repo_root=repo_root,
            )
        except MLFreshHybridCandidatePlanIngestError as e:
            print(f"ml-fresh-hybrid-candidate-plan-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-transfer-gap-review-worksheet":
        from pipeline.ml_transfer_gap_review_worksheet import (
            MLTransferGapReviewWorksheetError,
            run_ml_transfer_gap_review_worksheet_cli,
        )

        source_plan = Path(args.source_snapshot_candidate_plan) if args.source_snapshot_candidate_plan else None
        try:
            run_ml_transfer_gap_review_worksheet_cli(
                production_readiness_plan_path=Path(args.production_readiness_plan),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=Path(args.output),
                context_output_path=Path(args.context_output),
                markdown_output_path=Path(args.markdown_output),
                rows=int(args.rows),
                seed=int(args.seed),
                source_snapshot_candidate_plan_path=source_plan,
                corpus_snapshot_version=str(args.corpus_snapshot_version),
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
                ranking_run_id=args.ranking_run_id,
                embedding_version=args.embedding_version,
                cluster_version=args.cluster_version,
                database_url=args.database_url,
                mock_db=bool(args.mock_db),
            )
        except MLTransferGapReviewWorksheetError as e:
            print(f"ml-transfer-gap-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.context_output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return True

    if args.command == "ml-tiny-baseline":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_tiny_baseline import MLTinyBaselineError, run_ml_tiny_baseline_cli

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_tiny_baseline_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                family=str(args.family),
                target=str(args.target),
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLTinyBaselineError as e:
            print(f"ml-tiny-baseline: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-source-split-tiny-baseline":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_source_split_tiny_baseline import (
            MLSourceSplitTinyBaselineError,
            run_ml_source_split_tiny_baseline_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_source_split_tiny_baseline_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                ranking_run_id=rid,
                family=str(args.family),
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLSourceSplitTinyBaselineError as e:
            print(f"ml-source-split-tiny-baseline: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-source-split-error-analysis":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_source_split_error_analysis import (
            MLSourceSplitErrorAnalysisError,
            run_ml_source_split_error_analysis_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        top_n = int(getattr(args, "top_n", 10) or 10)
        if top_n < 1 or top_n > 100:
            parser.error("--top-n must be between 1 and 100")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_source_split_error_analysis_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                source_split_artifact_path=Path(args.source_split_artifact),
                ranking_run_id=rid,
                family=str(args.family),
                output_json=out_json,
                markdown_output=out_md,
                top_n=top_n,
            )
        except MLSourceSplitErrorAnalysisError as e:
            print(f"ml-source-split-error-analysis: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-fresh-product-candidate-ranking-source":
        from pipeline.ml_fresh_product_candidate_ranking_source import (
            MLFreshProductCandidateRankingSourceError,
            write_ml_fresh_product_candidate_ranking_source,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_product_candidate_ranking_source(
                fresh_eval_labeling_plan_path=Path(args.fresh_eval_labeling_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                database_url=args.database_url,
                family=str(args.family),
                ranking_run_id=args.ranking_run_id,
                min_confirmatory_candidate_works=args.min_confirmatory_candidate_works,
                source_version=str(args.source_version),
                repo_root=repo_root,
            )
        except MLFreshProductCandidateRankingSourceError as e:
            print(f"ml-fresh-product-candidate-ranking-source: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-tiny-baseline-rollup":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_tiny_baseline import MLTinyBaselineError
        from pipeline.ml_tiny_baseline_rollup import MLTinyBaselineRollupError, run_ml_tiny_baseline_rollup_cli

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_tiny_baseline_rollup_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                family=str(args.family),
                output_json=out_json,
                markdown_output=out_md,
            )
        except (MLTinyBaselineRollupError, MLTinyBaselineError) as e:
            code = getattr(e, "code", 2)
            print(f"ml-tiny-baseline-rollup: {e}", file=sys.stderr)
            raise SystemExit(code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-tiny-baseline-disagreement":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_tiny_baseline import MLTinyBaselineError
        from pipeline.ml_tiny_baseline_disagreement import (
            MLTinyBaselineDisagreementError,
            TARGET_ORDER as _disag_targets,
            run_ml_tiny_baseline_disagreement_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        if bool(getattr(args, "all_targets", False)) and getattr(args, "target", None):
            parser.error("use either --all-targets or --target, not both")
        if not getattr(args, "all_targets", False) and not getattr(args, "target", None):
            parser.error("provide --target or --all-targets")
        targets = tuple(_disag_targets) if getattr(args, "all_targets", False) else (str(args.target),)
        top_n = int(getattr(args, "top_n", 25) or 25)
        if top_n < 1 or top_n > 200:
            parser.error("--top-n must be between 1 and 200")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_tiny_baseline_disagreement_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                family=str(args.family),
                targets=targets,
                top_n=top_n,
                output_json=out_json,
                markdown_output=out_md,
            )
        except (MLTinyBaselineDisagreementError, MLTinyBaselineError) as e:
            code = getattr(e, "code", 2)
            print(f"ml-tiny-baseline-disagreement: {e}", file=sys.stderr)
            raise SystemExit(code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    return False
