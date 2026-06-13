from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_ml_bridge_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

    if args.command == "ml-label-readiness-matrix":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_label_readiness_matrix import MLLabelReadinessMatrixError, run_ml_label_readiness_matrix_cli

        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_label_readiness_matrix_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLLabelReadinessMatrixError as e:
            print(f"ml-label-readiness-matrix: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-bridge-recommendable-scorer-v1":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_bridge_recommendable_scorer import (
            MLOfflineBridgeRecommendableScorerError,
            run_ml_offline_bridge_recommendable_scorer_cli,
        )

        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_offline_bridge_recommendable_scorer_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                output_json=out_json,
                markdown_output=out_md,
                random_seed=int(args.random_seed),
            )
        except MLOfflineBridgeRecommendableScorerError as e:
            print(f"ml-offline-bridge-recommendable-scorer-v1: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-bridge-recommendable-scorer-v2":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_bridge_recommendable_scorer_v2 import (
            MLOfflineBridgeRecommendableScorerV2Error,
            run_ml_offline_bridge_recommendable_scorer_v2_cli,
        )

        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_offline_bridge_recommendable_scorer_v2_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                output_json=out_json,
                markdown_output=out_md,
                random_seed=int(args.random_seed),
            )
        except MLOfflineBridgeRecommendableScorerV2Error as e:
            print(f"ml-offline-bridge-recommendable-scorer-v2: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-bridge-recommendable-scorer-v3":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_bridge_recommendable_scorer_v3 import (
            MLOfflineBridgeRecommendableScorerV3Error,
            run_ml_offline_bridge_recommendable_scorer_v3_cli,
        )

        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_offline_bridge_recommendable_scorer_v3_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                v2_baseline_path=Path(args.v2_baseline) if args.v2_baseline else None,
                output_json=out_json,
                markdown_output=out_md,
                random_seed=int(args.random_seed),
            )
        except MLOfflineBridgeRecommendableScorerV3Error as e:
            print(f"ml-offline-bridge-recommendable-scorer-v3: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity import (
            MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError,
            run_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_cli,
        )

        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                v3_baseline_path=Path(args.v3_baseline),
                v2_baseline_path=Path(args.v2_baseline) if args.v2_baseline else None,
                output_json=out_json,
                markdown_output=out_md,
                v3_baseline_git_ref=args.v3_baseline_git_ref,
                random_seed=int(args.random_seed),
            )
        except MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError as e:
            print(
                f"ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity: {e}",
                file=sys.stderr,
            )
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-bounded-hybrid-bridge-eval-v1":
        from pipeline.ml_offline_bounded_hybrid_bridge_eval import (
            MLOfflineBoundedHybridBridgeEvalError,
            run_ml_offline_bounded_hybrid_bridge_eval_cli,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_offline_bounded_hybrid_bridge_eval_cli(
                label_dataset_path=Path(args.label_dataset),
                bridge_scorer_path=Path(args.bridge_scorer),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLOfflineBoundedHybridBridgeEvalError as e:
            print(f"ml-offline-bounded-hybrid-bridge-eval-v1: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-bridge-score-hybrid-eval":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_bridge_score_hybrid_eval import (
            MLOfflineBridgeScoreHybridEvalError,
            run_ml_offline_bridge_score_hybrid_eval_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_offline_bridge_score_hybrid_eval_cli(
                label_dataset_path=Path(args.label_dataset),
                v2_scorer_path=Path(args.v2_scorer),
                ranking_run_id=rid,
                database_url=args.database_url,
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLOfflineBridgeScoreHybridEvalError as e:
            print(f"ml-offline-bridge-score-hybrid-eval: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-bridge-hybrid-eval-v3":
        from pipeline.ml_offline_bridge_hybrid_eval_v3 import (
            MLOfflineBridgeHybridEvalV3Error,
            run_ml_offline_bridge_hybrid_eval_v3_cli,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        v2_path = Path(args.v2_scorer) if args.v2_scorer else None
        try:
            run_ml_offline_bridge_hybrid_eval_v3_cli(
                sensitivity_artifact_path=Path(args.sensitivity_artifact),
                label_dataset_path=Path(args.label_dataset),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                output_json=out_json,
                markdown_output=out_md,
                v2_scorer_path=v2_path,
            )
        except MLOfflineBridgeHybridEvalV3Error as e:
            print(f"ml-offline-bridge-hybrid-eval-v3: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-offline-bridge-hybrid-rank-pct-eval-v3":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_bridge_hybrid_rank_pct_eval_v3 import (
            MLOfflineBridgeHybridRankPctEvalV3Error,
            run_ml_offline_bridge_hybrid_rank_pct_eval_v3_cli,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        shadow_path = Path(args.shadow_pilot_artifact) if args.shadow_pilot_artifact else None
        linear_path = Path(args.linear_hybrid_eval_v3) if args.linear_hybrid_eval_v3 else None
        db_url = args.database_url or _bootstrap_loader.database_url_from_env()
        try:
            run_ml_offline_bridge_hybrid_rank_pct_eval_v3_cli(
                sensitivity_artifact_path=Path(args.sensitivity_artifact),
                label_dataset_path=Path(args.label_dataset),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                shadow_pilot_artifact_path=shadow_path,
                database_url=db_url,
                linear_hybrid_eval_v3_path=linear_path,
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLOfflineBridgeHybridRankPctEvalV3Error as e:
            print(f"ml-offline-bridge-hybrid-rank-pct-eval-v3: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-bridge-rank-pct-hybrid-controlled-rollout-eval":
        from pipeline.ml_bridge_rank_pct_hybrid_controlled_rollout_eval import (
            MLBridgeRankPctHybridControlledRolloutEvalError,
            run_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_cli,
        )
        from pipeline.ml_offline_bridge_hybrid_eval_v3 import MLOfflineBridgeHybridEvalV3Error
        from pipeline.ml_offline_bridge_hybrid_rank_pct_eval_v3 import (
            MLOfflineBridgeHybridRankPctEvalV3Error,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_cli(
                shadow_pilot_artifact_path=Path(args.shadow_pilot_artifact),
                sensitivity_artifact_path=Path(args.sensitivity_artifact),
                rank_pct_eval_artifact_path=Path(args.rank_pct_eval_artifact),
                label_dataset_path=Path(args.label_dataset),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                database_url=args.database_url,
                output_json=out_json,
                markdown_output=out_md,
            )
        except (
            MLBridgeRankPctHybridControlledRolloutEvalError,
            MLOfflineBridgeHybridEvalV3Error,
            MLOfflineBridgeHybridRankPctEvalV3Error,
        ) as e:
            print(f"ml-bridge-rank-pct-hybrid-controlled-rollout-eval: {e}", file=sys.stderr)
            raise SystemExit(getattr(e, "code", 2)) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-bridge-rank-pct-hybrid-serving-plan":
        from pipeline.ml_bridge_rank_pct_hybrid_serving_plan import (
            MLBridgeRankPctHybridServingPlanError,
            run_ml_bridge_rank_pct_hybrid_serving_plan_cli,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_bridge_rank_pct_hybrid_serving_plan_cli(
                controlled_rollout_eval_path=Path(args.controlled_rollout_eval),
                rank_pct_eval_artifact_path=Path(args.rank_pct_eval_artifact),
                linear_hybrid_eval_v3_path=Path(args.linear_hybrid_eval_v3),
                sensitivity_artifact_path=Path(args.sensitivity_artifact),
                label_dataset_path=Path(args.label_dataset),
                readiness_matrix_path=Path(args.readiness_matrix),
                embeddings_provenance_path=Path(args.embeddings_provenance),
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLBridgeRankPctHybridServingPlanError as e:
            print(f"ml-bridge-rank-pct-hybrid-serving-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-bridge-shadow-pilot":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_bridge_shadow_pilot import (
            MLBridgeShadowPilotError,
            run_ml_bridge_shadow_pilot_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        ws_csv = Path(args.worksheet_csv) if args.worksheet_csv else None
        ws_sidecar = Path(args.worksheet_sidecar) if args.worksheet_sidecar else None
        try:
            run_ml_bridge_shadow_pilot_cli(
                v2_scorer_path=Path(args.v2_scorer),
                ranking_run_id=rid,
                database_url=args.database_url,
                output_json=out_json,
                markdown_output=out_md,
                worksheet_csv=ws_csv,
                worksheet_sidecar=ws_sidecar,
            )
        except MLBridgeShadowPilotError as e:
            print(f"ml-bridge-shadow-pilot: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        if ws_csv is not None:
            print(ws_csv.resolve(), file=sys.stderr)
        if ws_sidecar is not None:
            print(ws_sidecar.resolve(), file=sys.stderr)
        return True

    if args.command == "ml-blind-family-context-eval":
        from pipeline.ml_blind_family_context_eval import (
            MLBlindFamilyContextEvalError,
            run_ml_blind_family_context_eval_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_blind_family_context_eval_cli(
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLBlindFamilyContextEvalError as e:
            print(f"ml-blind-family-context-eval: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return True

    return False
