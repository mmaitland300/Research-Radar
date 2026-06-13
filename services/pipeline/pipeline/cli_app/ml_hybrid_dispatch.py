from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_ml_hybrid_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

    if args.command == "ml-learned-scorer-holdout-policy":
        from pipeline.ml_learned_scorer_holdout_policy import (
            MLLearnedScorerHoldoutPolicyError,
            write_ml_learned_scorer_holdout_policy,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_learned_scorer_holdout_policy(
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                embeddings_path=Path(args.embeddings),
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                output_path=out_json,
                markdown_output_path=out_md,
                policy_version=str(args.policy_version),
                repo_root=repo_root,
            )
        except MLLearnedScorerHoldoutPolicyError as e:
            print(f"ml-learned-scorer-holdout-policy: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-learned-scorer-holdout-assignment":
        from pipeline.ml_learned_scorer_holdout_assignment import (
            MLLearnedScorerHoldoutAssignmentError,
            write_ml_learned_scorer_holdout_assignment,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_learned_scorer_holdout_assignment(
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                embeddings_path=Path(args.embeddings),
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                holdout_policy_path=Path(args.holdout_policy),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                output_path=out_json,
                markdown_output_path=out_md,
                assignment_version=str(args.assignment_version),
                strategy_id=str(args.strategy_id),
                seed=args.seed,
                repo_root=repo_root,
            )
        except MLLearnedScorerHoldoutAssignmentError as e:
            print(f"ml-learned-scorer-holdout-assignment: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-hybrid-scorer-offline-experiment-spec":
        from pipeline.ml_hybrid_scorer_offline_experiment_spec import (
            MLHybridScorerOfflineExperimentSpecError,
            write_ml_hybrid_scorer_offline_experiment_spec,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_hybrid_scorer_offline_experiment_spec(
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                holdout_assignment_path=Path(args.holdout_assignment),
                split_policy_path=Path(args.split_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                label_dataset_path=Path(args.label_dataset) if args.label_dataset else None,
                holdout_policy_path=Path(args.holdout_policy) if args.holdout_policy else None,
                audit_embedding_scorer_export_path=(
                    Path(args.audit_embedding_scorer_export) if args.audit_embedding_scorer_export else None
                ),
                output_path=out_json,
                markdown_output_path=out_md,
                spec_version=str(args.spec_version),
                repo_root=repo_root,
            )
        except MLHybridScorerOfflineExperimentSpecError as e:
            print(f"ml-hybrid-scorer-offline-experiment-spec: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-hybrid-scorer-offline-experiment":
        from pipeline.ml_hybrid_scorer_offline_experiment import (
            MLHybridScorerOfflineExperimentError,
            write_ml_hybrid_scorer_offline_experiment,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_hybrid_scorer_offline_experiment(
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                experiment_spec_path=Path(args.experiment_spec),
                holdout_assignment_path=Path(args.holdout_assignment),
                holdout_policy_path=Path(args.holdout_policy) if args.holdout_policy else None,
                output_path=out_json,
                markdown_output_path=out_md,
                experiment_version=str(args.experiment_version),
                repo_root=repo_root,
            )
        except MLHybridScorerOfflineExperimentError as e:
            print(f"ml-hybrid-scorer-offline-experiment: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-hybrid-scorer-metric-gates":
        from pipeline.ml_hybrid_scorer_metric_gates import (
            MLHybridScorerMetricGatesError,
            write_ml_hybrid_scorer_metric_gates,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_hybrid_scorer_metric_gates(
                hybrid_experiment_path=Path(args.hybrid_experiment),
                experiment_spec_path=Path(args.experiment_spec),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                holdout_assignment_path=Path(args.holdout_assignment) if args.holdout_assignment else None,
                output_path=out_json,
                markdown_output_path=out_md,
                gates_version=str(args.gates_version),
                repo_root=repo_root,
            )
        except MLHybridScorerMetricGatesError as e:
            print(f"ml-hybrid-scorer-metric-gates: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-fresh-eval-surface-policy-hybrid":
        from pipeline.ml_fresh_eval_surface_policy_hybrid import (
            MLFreshEvalSurfacePolicyHybridError,
            write_ml_fresh_eval_surface_policy_hybrid,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_eval_surface_policy_hybrid(
                hybrid_metric_gates_path=Path(args.hybrid_metric_gates),
                hybrid_experiment_path=Path(args.hybrid_experiment),
                hybrid_experiment_spec_path=Path(args.hybrid_experiment_spec),
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                holdout_assignment_path=Path(args.holdout_assignment),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                policy_version=str(args.policy_version),
                repo_root=repo_root,
            )
        except MLFreshEvalSurfacePolicyHybridError as e:
            print(f"ml-fresh-eval-surface-policy-hybrid: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-fresh-eval-surface-hybrid-materialize":
        from pipeline.ml_fresh_eval_surface_hybrid_materialize import (
            MLFreshEvalSurfaceHybridMaterializeError,
            write_ml_fresh_eval_surface_hybrid_materialize,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_eval_surface_hybrid_materialize(
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                ranking_run_id=args.ranking_run_id,
                family=str(args.family),
                corpus_snapshot_version=args.corpus_snapshot_version,
                database_url=args.database_url,
                surface_version=str(args.surface_version),
                expected_label_dataset_version=str(args.expected_label_dataset_version),
                repo_root=repo_root,
            )
        except MLFreshEvalSurfaceHybridMaterializeError as e:
            print(f"ml-fresh-eval-surface-hybrid-materialize: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-hybrid-validation-on-fresh-surface":
        from pipeline.ml_hybrid_validation_on_fresh_surface import (
            MLHybridValidationOnFreshSurfaceError,
            write_ml_hybrid_validation_on_fresh_surface,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_hybrid_validation_on_fresh_surface(
                fresh_eval_surface_path=Path(args.fresh_eval_surface),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                audit_embedding_scorer_export_path=Path(args.audit_embedding_scorer_export),
                fresh_hybrid_snapshot_embeddings_path=Path(args.fresh_hybrid_snapshot_embeddings),
                hybrid_experiment_spec_path=Path(args.hybrid_experiment_spec) if args.hybrid_experiment_spec else None,
                hybrid_metric_gates_path=Path(args.hybrid_metric_gates) if args.hybrid_metric_gates else None,
                database_url=args.database_url,
                output_path=out_json,
                markdown_output_path=out_md,
                validation_version=str(args.validation_version),
                repo_root=repo_root,
            )
        except MLHybridValidationOnFreshSurfaceError as e:
            print(f"ml-hybrid-validation-on-fresh-surface: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        _print_artifact_values(
            out_json,
            ("validation_scope", "candidate_pool_work_count"),
            ("validation_scope", "confirmatory_metric_work_count"),
            ("recommended_next_stage",),
        )
        return True

    if args.command == "ml-hybrid-validation-metric-gates":
        from pipeline.ml_hybrid_validation_metric_gates import (
            MLHybridValidationMetricGatesError,
            write_ml_hybrid_validation_metric_gates,
        )

        if not args.hybrid_validation_on_fresh_surface:
            parser.error("--hybrid-validation-on-fresh-surface is required")
        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_hybrid_validation_metric_gates(
                hybrid_validation_on_fresh_surface_path=Path(args.hybrid_validation_on_fresh_surface),
                fresh_eval_surface_path=Path(args.fresh_eval_surface),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                output_path=out_json,
                markdown_output_path=out_md,
                gates_version=str(args.gates_version),
                hybrid_experiment_spec_path=Path(args.hybrid_experiment_spec) if args.hybrid_experiment_spec else None,
                hybrid_scorer_metric_gates_path=Path(args.hybrid_scorer_metric_gates)
                if args.hybrid_scorer_metric_gates
                else None,
                repo_root=repo_root,
            )
        except MLHybridValidationMetricGatesError as e:
            print(f"ml-hybrid-validation-metric-gates: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(payload["primary_confirmatory_arm"])
        print(payload["confirmatory_validation_passed"])
        print(payload["recommended_next_stage"])
        return True

    if args.command == "ml-shadow-scorer-v1-audit":
        from pipeline.ml_shadow_scorer_v1 import (
            MLShadowScorerV1Error,
            write_ml_shadow_scorer_v1_audit,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_v1_audit(
                shadow_scorer_spec_path=Path(args.shadow_scorer_spec),
                hybrid_validation_on_fresh_surface_path=Path(args.hybrid_validation_on_fresh_surface),
                fresh_eval_surface_path=Path(args.fresh_eval_surface),
                output_path=out_json,
                markdown_output_path=out_md,
                implementation_version=str(args.implementation_version),
                repo_root=repo_root,
            )
        except MLShadowScorerV1Error as e:
            print(f"ml-shadow-scorer-v1-audit: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(payload["implementation_status"]["implementation_matches_spec"])
        print(payload["implementation_status"]["implementation_matches_validation_replay"])
        print(payload["recommended_next_stage"])
        return True

    if args.command == "ml-shadow-scorer-v1-audit-output":
        from pipeline.ml_shadow_scorer_v1 import (
            MLShadowScorerV1Error,
            write_ml_shadow_scorer_v1_audit_output,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_shadow_scorer_v1_audit_output(
                shadow_scorer_execution_readiness_gates_path=Path(
                    args.shadow_scorer_execution_readiness_gates
                ),
                shadow_scorer_implementation_path=Path(args.shadow_scorer_implementation),
                shadow_scorer_spec_path=Path(args.shadow_scorer_spec),
                hybrid_validation_on_fresh_surface_path=Path(args.hybrid_validation_on_fresh_surface),
                output_path=out_json,
                markdown_output_path=out_md,
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLShadowScorerV1Error as e:
            print(f"ml-shadow-scorer-v1-audit-output: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(payload["execution_summary"]["status"])
        print(payload["execution_verification"]["output_matches_validation_replay"])
        print(payload["recommended_next_stage"])
        return True

    return False
