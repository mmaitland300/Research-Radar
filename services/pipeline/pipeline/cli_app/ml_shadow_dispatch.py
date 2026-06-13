from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_ml_shadow_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

    if args.command == "ml-shadow-scorer-generalization-second-surface":
        from pipeline.ml_shadow_scorer_generalization_second_surface import (
            MLShadowScorerGeneralizationSecondSurfaceError,
            write_ml_shadow_scorer_generalization_second_surface,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        first_surface = Path(args.first_validated_surface) if args.first_validated_surface else None
        try:
            payload = write_ml_shadow_scorer_generalization_second_surface(
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                online_shadow_policy_path=Path(args.online_shadow_policy),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                offline_production_candidate_scoring_v3_path=Path(args.offline_production_candidate_scoring_v3),
                first_validated_surface_path=first_surface,
                database_url=str(args.database_url) if args.database_url else None,
                ranking_run_id=str(args.ranking_run_id) if args.ranking_run_id else None,
                family=str(args.family),
                output_path=out_json,
                markdown_output_path=out_md,
                surface_version=str(args.surface_version),
                repo_root=repo_root,
            )
        except MLShadowScorerGeneralizationSecondSurfaceError as e:
            print(f"ml-shadow-scorer-generalization-second-surface: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        _print_artifact_values(
            out_json,
            ("discovery_summary", "status"),
            ("readiness_for_generalization_audit", "ready_for_generalization_audit_execution"),
            ("recommended_next_stage",),
        )
        return True

    if args.command == "ml-shadow-scorer-second-surface-labeling-worksheet":
        from pipeline.ml_shadow_scorer_second_surface_labeling_worksheet import (
            MLShadowScorerSecondSurfaceLabelingWorksheetError,
            write_ml_shadow_scorer_second_surface_labeling_worksheet,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_shadow_scorer_second_surface_labeling_worksheet(
                generalization_second_surface_path=Path(args.generalization_second_surface),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                offline_production_candidate_scoring_v3_path=Path(args.offline_production_candidate_scoring_v3),
                first_validated_surface_path=Path(args.first_validated_surface),
                fresh_surface_policy_path=Path(args.fresh_surface_policy) if args.fresh_surface_policy else None,
                database_url=str(args.database_url) if args.database_url else None,
                requested_rows=int(args.requested_rows),
                worksheet_version=str(args.worksheet_version),
                review_pool_variant=str(args.review_pool_variant),
                seed=int(args.seed),
                output_path=Path(args.output),
                context_output_path=Path(args.context_output),
                markdown_output_path=Path(args.markdown_output),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondSurfaceLabelingWorksheetError as e:
            print(f"ml-shadow-scorer-second-surface-labeling-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.context_output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        _print_artifact_values(
            Path(args.context_output),
            ("selection_summary", "selected_row_count"),
            ("recommended_next_stage",),
        )
        return True

    if args.command == "ml-shadow-scorer-second-candidate-source-expansion-plan":
        from pipeline.ml_shadow_scorer_second_candidate_source_expansion_plan import (
            MLShadowScorerSecondCandidateSourceExpansionPlanError,
            write_ml_shadow_scorer_second_candidate_source_expansion_plan,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_second_candidate_source_expansion_plan(
                generalization_second_surface_path=Path(args.generalization_second_surface),
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                online_shadow_policy_path=Path(args.online_shadow_policy),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondCandidateSourceExpansionPlanError as e:
            print(f"ml-shadow-scorer-second-candidate-source-expansion-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(payload["current_blocker_summary"]["candidate_gap"])
        print(payload["recommended_next_stage"])
        return True

    if args.command == "ml-shadow-scorer-second-surface-learned-probability-coverage-plan":
        from pipeline.ml_shadow_scorer_second_surface_learned_probability_coverage_plan import (
            MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError,
            write_ml_shadow_scorer_second_surface_learned_probability_coverage_plan,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_second_surface_learned_probability_coverage_plan(
                generalization_second_surface_path=Path(args.generalization_second_surface),
                label_dataset_path=Path(args.label_dataset),
                second_snapshot_embeddings_path=Path(args.second_snapshot_embeddings),
                offline_audit_embedding_scorer_path=Path(args.offline_audit_embedding_scorer),
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                online_shadow_policy_path=Path(args.online_shadow_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError as e:
            print(f"ml-shadow-scorer-second-surface-learned-probability-coverage-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(payload["evidence_summary"]["learned_probability_coverage"]["learned_probability_coverage_count"])
        print(payload["recommended_next_stage"])
        return True

    if args.command == "ml-shadow-scorer-second-surface-learned-probability-apply":
        from pipeline.ml_shadow_scorer_second_surface_learned_probability_apply import (
            MLShadowScorerSecondSurfaceLearnedProbabilityApplyError,
            write_ml_shadow_scorer_second_surface_learned_probability_apply,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_second_surface_learned_probability_apply(
                learned_probability_coverage_plan_path=Path(args.learned_probability_coverage_plan),
                generalization_second_surface_path=Path(args.generalization_second_surface),
                label_dataset_path=Path(args.label_dataset),
                second_snapshot_embeddings_path=Path(args.second_snapshot_embeddings),
                offline_audit_embedding_scorer_path=Path(args.offline_audit_embedding_scorer),
                database_url=args.database_url,
                ranking_run_id=str(args.ranking_run_id),
                family=str(args.family),
                corpus_snapshot_version=str(args.corpus_snapshot_version),
                embedding_version=str(args.embedding_version),
                output_path=out_json,
                markdown_output_path=out_md,
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondSurfaceLearnedProbabilityApplyError as e:
            print(f"ml-shadow-scorer-second-surface-learned-probability-apply: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        _print_artifact_values(
            out_json,
            ("execution_summary", "learned_probability_coverage_count"),
            ("recommended_next_stage",),
        )
        return True

    if args.command == "ml-shadow-scorer-second-surface-generalization-audit":
        from pipeline.ml_shadow_scorer_second_surface_generalization_audit import (
            MLShadowScorerSecondSurfaceGeneralizationAuditError,
            write_ml_shadow_scorer_second_surface_generalization_audit,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_second_surface_generalization_audit(
                generalization_second_surface_path=Path(args.generalization_second_surface),
                learned_probability_artifact_path=Path(args.learned_probability_artifact),
                label_dataset_path=Path(args.label_dataset),
                shadow_scorer_spec_path=Path(args.shadow_scorer_spec),
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                online_shadow_policy_path=Path(args.online_shadow_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondSurfaceGeneralizationAuditError as e:
            print(f"ml-shadow-scorer-second-surface-generalization-audit: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(payload["audit_scope"]["confirmatory_metric_work_count"])
        print(payload["recommended_next_stage"])
        return True

    if args.command == "ml-shadow-scorer-online-shadow-runtime-disabled":
        from pipeline.ml_shadow_scorer_online_shadow_runtime import (
            MLShadowScorerOnlineShadowRuntimeError,
            write_ml_shadow_scorer_online_shadow_runtime_disabled,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_online_shadow_runtime_disabled(
                generalization_audit_gates_path=Path(args.generalization_audit_gates),
                second_surface_generalization_audit_path=Path(args.second_surface_generalization_audit),
                online_shadow_policy_path=Path(args.online_shadow_policy),
                shadow_scorer_spec_path=Path(args.shadow_scorer_spec),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                output_path=out_json,
                markdown_output_path=out_md,
                runtime_version=str(args.runtime_version),
                repo_root=repo_root,
            )
        except MLShadowScorerOnlineShadowRuntimeError as e:
            print(f"ml-shadow-scorer-online-shadow-runtime-disabled: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(payload["last_disabled_run"]["status"])
        print(payload["recommended_next_stage"])
        return True

    if args.command == "ml-shadow-scorer-second-hybrid-candidate-plan":
        from pipeline.ml_shadow_scorer_second_hybrid_candidate_plan import (
            MLShadowScorerSecondHybridCandidatePlanError,
            write_ml_shadow_scorer_second_hybrid_candidate_plan,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_second_hybrid_candidate_plan(
                second_candidate_source_expansion_plan_path=Path(args.second_candidate_source_expansion_plan),
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                offline_production_candidate_scoring_v3_path=Path(args.offline_production_candidate_scoring_v3),
                first_validated_surface_path=Path(args.first_validated_surface),
                generalization_second_surface_path=Path(args.generalization_second_surface),
                target_min=int(args.target_min),
                target_max=int(args.target_max),
                mailto=args.mailto,
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondHybridCandidatePlanError as e:
            print(f"ml-shadow-scorer-second-hybrid-candidate-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(payload["candidate_selection"]["selected_total"])
        print(payload["readiness_estimate"]["estimated_confirmatory_eligible_after_exclusions"])
        print(payload["recommended_next_stage"])
        return True

    if args.command == "ml-shadow-scorer-second-candidate-plan-ingest":
        from pipeline.ml_shadow_scorer_second_candidate_plan_ingest import (
            MLShadowScorerSecondCandidatePlanIngestError,
            write_ml_shadow_scorer_second_candidate_plan_ingest,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_second_candidate_plan_ingest(
                second_hybrid_candidate_plan_path=Path(args.second_hybrid_candidate_plan),
                generalization_audit_plan_path=Path(args.generalization_audit_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                snapshot_version=args.snapshot_version,
                database_url=args.database_url,
                ingest_version=str(args.ingest_version),
                dry_run=bool(args.dry_run),
                repo_root=repo_root,
            )
        except MLShadowScorerSecondCandidatePlanIngestError as e:
            print(f"ml-shadow-scorer-second-candidate-plan-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        _print_artifact_values(
            out_json,
            ("ingest_result", "status"),
            ("ingest_result", "snapshot_work_count"),
            ("recommended_next_stage",),
        )
        return True

    return False
