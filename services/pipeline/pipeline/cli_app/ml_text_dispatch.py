from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_ml_text_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

    if args.command == "ml-offline-baseline-eval":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_baseline_eval import MLOfflineBaselineEvalError, run_ml_offline_baseline_eval_cli

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_offline_baseline_eval_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLOfflineBaselineEvalError as e:
            print(f"ml-offline-baseline-eval: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-external-feature-coverage":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_external_feature_coverage import (
            MLExternalFeatureCoverageError,
            run_ml_external_feature_coverage_cli,
        )

        emb = (args.embedding_version or "").strip()
        if not emb:
            parser.error("--embedding-version must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        sidecar = Path(args.context_sidecar) if args.context_sidecar else None
        try:
            run_ml_external_feature_coverage_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                context_sidecar_path=sidecar,
                embedding_version=emb,
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLExternalFeatureCoverageError as e:
            print(f"ml-external-feature-coverage: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-external-text-corpus":
        from pipeline.ml_external_text_corpus import (
            MLExternalTextCorpusError,
            run_ml_external_text_corpus_cli,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        sidecar = Path(args.context_sidecar) if args.context_sidecar else None
        try:
            run_ml_external_text_corpus_cli(
                label_dataset_path=Path(args.label_dataset),
                context_sidecar_path=sidecar,
                output_json=out_json,
                markdown_output=out_md,
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
            )
        except MLExternalTextCorpusError as e:
            print(f"ml-external-text-corpus: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-labeled-text-corpus":
        from pipeline.ml_labeled_text_corpus import (
            MLLabeledTextCorpusError,
            write_ml_labeled_text_corpus,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        external = Path(args.external_text_corpus) if args.external_text_corpus else None
        try:
            write_ml_labeled_text_corpus(
                label_dataset_path=Path(args.label_dataset),
                external_text_corpus_path=external,
                output_path=out_json,
                markdown_output_path=out_md,
                corpus_version=str(args.corpus_version),
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
                max_rows=args.max_rows,
            )
        except MLLabeledTextCorpusError as e:
            print(f"ml-labeled-text-corpus: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-labeled-text-corpus-normalize":
        from pipeline.ml_labeled_text_corpus_normalize import (
            MLLabeledTextCorpusNormalizeError,
            write_ml_labeled_text_corpus_normalize,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_ml_labeled_text_corpus_normalize(
                source_corpus_path=Path(args.source_corpus),
                output_path=out_json,
                markdown_output_path=out_md,
                source_corpus_version=str(args.source_corpus_version),
                corpus_version=str(args.corpus_version),
            )
        except MLLabeledTextCorpusNormalizeError as e:
            print(f"ml-labeled-text-corpus-normalize: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-external-text-embeddings":
        from pipeline.ml_external_text_embeddings import (
            MLExternalTextEmbeddingsError,
            write_external_text_embeddings,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_external_text_embeddings(
                text_corpus_path=Path(args.text_corpus),
                output_path=out_json,
                markdown_output_path=out_md,
                embedding_model=str(args.embedding_model),
                expected_dimensions=int(args.expected_dimensions),
                batch_size=int(args.batch_size),
                mock_embeddings=bool(args.mock_embeddings),
            )
        except MLExternalTextEmbeddingsError as e:
            print(f"ml-external-text-embeddings: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-text-only-baseline":
        from pipeline.ml_text_only_baseline import (
            MLTextOnlyBaselineError,
            write_ml_text_only_baseline,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_ml_text_only_baseline(
                embeddings_path=Path(args.embeddings),
                label_dataset_path=Path(args.label_dataset),
                output_path=out_json,
                markdown_output_path=out_md,
                random_seed=int(args.random_seed),
                cv_folds=int(args.cv_folds),
            )
        except MLTextOnlyBaselineError as e:
            print(f"ml-text-only-baseline: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-labeled-text-embeddings":
        from pipeline.ml_labeled_text_embeddings import (
            MLLabeledTextEmbeddingsError,
            write_ml_labeled_text_embeddings,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_ml_labeled_text_embeddings(
                text_corpus_path=Path(args.text_corpus),
                output_path=out_json,
                markdown_output_path=out_md,
                source_corpus_version=str(args.source_corpus_version),
                embedding_artifact_version=str(args.embedding_artifact_version),
                embedding_model=str(args.embedding_model),
                expected_dimensions=int(args.expected_dimensions),
                batch_size=int(args.batch_size),
                mock_embeddings=bool(args.mock_embeddings),
            )
        except MLLabeledTextEmbeddingsError as e:
            print(f"ml-labeled-text-embeddings: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-text-baseline-cross-pool":
        from pipeline.ml_text_baseline_cross_pool import (
            MLTextBaselineCrossPoolError,
            write_ml_text_baseline_cross_pool,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_ml_text_baseline_cross_pool(
                embeddings_path=Path(args.embeddings),
                label_dataset_path=Path(args.label_dataset),
                output_path=out_json,
                markdown_output_path=out_md,
                expected_embedding_artifact_version=str(args.expected_embedding_artifact_version),
                expected_label_dataset_version=str(args.expected_label_dataset_version),
                baseline_version=str(args.baseline_version),
                random_seed=int(args.random_seed),
            )
        except MLTextBaselineCrossPoolError as e:
            print(f"ml-text-baseline-cross-pool: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-text-transfer-readiness":
        from pipeline.ml_text_transfer_readiness import (
            MLTextTransferReadinessError,
            write_ml_text_transfer_readiness,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        text_corpus_v2 = Path(args.text_corpus_v2) if args.text_corpus_v2 else None
        embeddings_v1 = Path(args.embeddings_v1) if args.embeddings_v1 else None
        try:
            write_ml_text_transfer_readiness(
                cross_pool_path=Path(args.cross_pool),
                label_dataset_path=Path(args.label_dataset),
                text_corpus_v2_path=text_corpus_v2,
                embeddings_v1_path=embeddings_v1,
                output_path=out_json,
                markdown_output_path=out_md,
                expected_cross_pool_version=str(args.expected_cross_pool_version),
                expected_label_dataset_version=str(args.expected_label_dataset_version),
                expected_text_corpus_version=str(args.expected_text_corpus_version),
                expected_embeddings_version=str(args.expected_embeddings_version),
                readiness_version=str(args.readiness_version),
            )
        except MLTextTransferReadinessError as e:
            print(f"ml-text-transfer-readiness: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-production-readiness-plan":
        from pipeline.ml_production_readiness_plan import (
            MLProductionReadinessPlanError,
            write_ml_production_readiness_plan,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        conflict_policy = Path(args.conflict_policy) if args.conflict_policy else None
        cross_pool = Path(args.cross_pool) if args.cross_pool else None
        text_corpus_v2 = Path(args.text_corpus_v2) if args.text_corpus_v2 else None
        embeddings_v1 = Path(args.embeddings_v1) if args.embeddings_v1 else None
        try:
            write_ml_production_readiness_plan(
                transfer_readiness_path=Path(args.transfer_readiness),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=conflict_policy,
                cross_pool_path=cross_pool,
                text_corpus_v2_path=text_corpus_v2,
                embeddings_v1_path=embeddings_v1,
                output_path=out_json,
                markdown_output_path=out_md,
            )
        except MLProductionReadinessPlanError as e:
            print(f"ml-production-readiness-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-label-split-policy":
        from pipeline.ml_label_split_policy import MLLabelSplitPolicyError, write_ml_label_split_policy

        transfer_readiness = Path(args.transfer_readiness) if args.transfer_readiness else None
        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_label_split_policy(
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                transfer_readiness_path=transfer_readiness,
                output_path=out_json,
                markdown_output_path=out_md,
                policy_version=str(args.policy_version),
                repo_root=repo_root,
            )
        except MLLabelSplitPolicyError as e:
            print(f"ml-label-split-policy: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-ranker-experiment":
        from pipeline.ml_offline_ranker_experiment import (
            MLOfflineRankerExperimentError,
            write_ml_offline_ranker_experiment,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_offline_ranker_experiment(
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                embeddings_path=Path(args.embeddings),
                output_path=out_json,
                markdown_output_path=out_md,
                target=str(args.target),
                random_seed=args.random_seed,
                cv_folds=int(args.cv_folds),
                repo_root=repo_root,
            )
        except MLOfflineRankerExperimentError as e:
            print(f"ml-offline-ranker-experiment: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-metric-gates":
        from pipeline.ml_offline_metric_gates import MLOfflineMetricGatesError, write_ml_offline_metric_gates

        repo_root = Path(args.repo_root) if args.repo_root else None
        transfer_readiness = Path(args.transfer_readiness) if args.transfer_readiness else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_offline_metric_gates(
                ranker_experiment_path=Path(args.ranker_experiment),
                split_policy_path=Path(args.split_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                transfer_readiness_path=transfer_readiness,
                output_path=out_json,
                markdown_output_path=out_md,
                gates_version=str(args.gates_version),
                repo_root=repo_root,
            )
        except MLOfflineMetricGatesError as e:
            print(f"ml-offline-metric-gates: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-production-candidate-scoring":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_production_candidate_scoring import (
            MLOfflineProductionCandidateScoringError,
            run_ml_offline_production_candidate_scoring_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        repo_root = Path(args.repo_root) if args.repo_root else None
        audit_embedding_scorer_export = (
            Path(args.audit_embedding_scorer_export) if args.audit_embedding_scorer_export else None
        )
        holdout_assignment = Path(args.holdout_assignment) if args.holdout_assignment else None
        holdout_policy = Path(args.holdout_policy) if args.holdout_policy else None
        gates_v2 = Path(args.production_candidate_metric_gates_v2) if args.production_candidate_metric_gates_v2 else None
        if args.scoring_mode == "heuristic_and_audit_embedding_scorer" and audit_embedding_scorer_export is None:
            parser.error("--audit-embedding-scorer-export is required when --scoring-mode heuristic_and_audit_embedding_scorer")
        if args.scoring_mode == "heuristic_and_holdout_embedding_scorer":
            if audit_embedding_scorer_export is None:
                parser.error("--audit-embedding-scorer-export is required when --scoring-mode heuristic_and_holdout_embedding_scorer")
            if holdout_assignment is None:
                parser.error("--holdout-assignment is required when --scoring-mode heuristic_and_holdout_embedding_scorer")
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            run_ml_offline_production_candidate_scoring_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                metric_gates_path=Path(args.metric_gates),
                audit_ranker_experiment_path=Path(args.audit_ranker_experiment),
                embeddings_path=Path(args.embeddings),
                ranking_run_id=rid,
                family=str(args.family),
                target=str(args.target),
                output_path=out_json,
                markdown_output_path=out_md,
                experiment_version=str(args.experiment_version) if args.experiment_version else None,
                scoring_mode=str(args.scoring_mode),
                audit_embedding_scorer_export_path=audit_embedding_scorer_export,
                holdout_assignment_path=holdout_assignment,
                holdout_policy_path=holdout_policy,
                production_candidate_metric_gates_v2_path=gates_v2,
                repo_root=repo_root,
            )
        except MLOfflineProductionCandidateScoringError as e:
            print(f"ml-offline-production-candidate-scoring: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-production-candidate-metric-gates":
        from pipeline.ml_offline_production_candidate_metric_gates import (
            MLOfflineProductionCandidateMetricGatesError,
            write_ml_offline_production_candidate_metric_gates,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        audit_scorer_export = (
            Path(args.audit_embedding_scorer_export) if args.audit_embedding_scorer_export else None
        )
        holdout_assignment = Path(args.holdout_assignment) if args.holdout_assignment else None
        holdout_policy = Path(args.holdout_policy) if args.holdout_policy else None
        prior_v1_gates = (
            Path(args.production_candidate_metric_gates_v1)
            if args.production_candidate_metric_gates_v1
            else None
        )
        prior_v2_gates = (
            Path(args.production_candidate_metric_gates_v2)
            if args.production_candidate_metric_gates_v2
            else None
        )
        if (
            str(args.gates_version) == "ml-offline-production-candidate-metric-gates-v2"
            and audit_scorer_export is None
        ):
            parser.error(
                "--audit-embedding-scorer-export is required when --gates-version "
                "ml-offline-production-candidate-metric-gates-v2"
            )
        if str(args.gates_version) == "ml-offline-production-candidate-metric-gates-v3":
            if audit_scorer_export is None:
                parser.error(
                    "--audit-embedding-scorer-export is required when --gates-version "
                    "ml-offline-production-candidate-metric-gates-v3"
                )
            if holdout_assignment is None:
                parser.error(
                    "--holdout-assignment is required when --gates-version "
                    "ml-offline-production-candidate-metric-gates-v3"
                )
            if holdout_policy is None:
                parser.error(
                    "--holdout-policy is required when --gates-version "
                    "ml-offline-production-candidate-metric-gates-v3"
                )
        try:
            write_ml_offline_production_candidate_metric_gates(
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                offline_metric_gates_path=Path(args.offline_metric_gates),
                split_policy_path=Path(args.split_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                audit_embedding_scorer_export_path=audit_scorer_export,
                production_candidate_metric_gates_v1_path=prior_v1_gates,
                production_candidate_metric_gates_v2_path=prior_v2_gates,
                holdout_assignment_path=holdout_assignment,
                holdout_policy_path=holdout_policy,
                output_path=out_json,
                markdown_output_path=out_md,
                gates_version=str(args.gates_version),
                repo_root=repo_root,
            )
        except MLOfflineProductionCandidateMetricGatesError as e:
            print(f"ml-offline-production-candidate-metric-gates: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-audit-embedding-scorer-export":
        from pipeline.ml_offline_audit_embedding_scorer_export import (
            MLOfflineAuditEmbeddingScorerExportError,
            write_ml_offline_audit_embedding_scorer_export,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        ranker_experiment = Path(args.ranker_experiment) if args.ranker_experiment else None
        fit_mode = str(args.fit_mode)
        production_candidate_metric_gates = (
            Path(args.production_candidate_metric_gates) if args.production_candidate_metric_gates else None
        )
        holdout_assignment = Path(args.holdout_assignment) if args.holdout_assignment else None
        holdout_policy = Path(args.holdout_policy) if args.holdout_policy else None
        v1_reference = (
            Path(args.audit_embedding_scorer_export_v1) if args.audit_embedding_scorer_export_v1 else None
        )
        if fit_mode == "full_fit_audit_corpus" and production_candidate_metric_gates is None:
            parser.error(
                "ml-offline-audit-embedding-scorer-export: --production-candidate-metric-gates is required "
                "when --fit-mode full_fit_audit_corpus"
            )
        if fit_mode == "holdout_bound_train_only" and (holdout_assignment is None or holdout_policy is None):
            parser.error(
                "ml-offline-audit-embedding-scorer-export: --holdout-assignment and --holdout-policy are required "
                "when --fit-mode holdout_bound_train_only"
            )
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_offline_audit_embedding_scorer_export(
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                embeddings_path=Path(args.embeddings),
                production_candidate_metric_gates_path=production_candidate_metric_gates,
                holdout_assignment_path=holdout_assignment,
                holdout_policy_path=holdout_policy,
                audit_embedding_scorer_export_v1_path=v1_reference,
                ranker_experiment_path=ranker_experiment,
                output_path=out_json,
                markdown_output_path=out_md,
                target=str(args.target),
                random_seed=args.random_seed,
                scorer_version=str(args.scorer_version) if args.scorer_version else None,
                fit_mode=fit_mode,
                repo_root=repo_root,
            )
        except MLOfflineAuditEmbeddingScorerExportError as e:
            print(f"ml-offline-audit-embedding-scorer-export: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return True

    return False
