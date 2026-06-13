from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

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
    BRIDGE_ELIGIBILITY_MODE_CURRENT,
    SUPPORTED_BRIDGE_ELIGIBILITY_MODES,
    MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY,
    execute_ranking_run,
    validate_bridge_eligibility_mode,
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
from pipeline.bridge_objective_experiment_compare import (
    BridgeObjectiveExperimentCompareError,
    run_bridge_objective_experiment_compare,
)
from pipeline.bridge_objective_label_coverage import (
    BridgeObjectiveLabelCoverageError,
    run_bridge_objective_label_coverage,
)
from pipeline.bridge_objective_labeled_outcome import (
    BridgeObjectiveLabeledOutcomeError,
    run_bridge_objective_labeled_outcome,
)
from pipeline.bridge_weight_experiment_compare import (
    BridgeWeightExperimentCompareError,
    run_bridge_weight_experiment_compare,
)
from pipeline.bridge_weight_experiment_delta_worksheet import (
    BridgeWeightExperimentDeltaWorksheetError,
    write_bridge_weight_experiment_delta_worksheet,
)
from pipeline.bridge_weight_experiment_delta_summary import (
    BridgeWeightExperimentDeltaSummaryError,
    run_bridge_weight_experiment_delta_summary,
)
from pipeline.bridge_weight_response_rollup import (
    BridgeWeightResponseRollupError,
    run_bridge_weight_response_rollup,
)
from pipeline.bridge_weight_labeled_outcome import (
    BridgeWeightLabeledOutcomeError,
    run_bridge_weight_labeled_outcome,
)
from pipeline.bridge_eligibility_sensitivity import (
    BridgeEligibilitySensitivityError,
    run_bridge_eligibility_sensitivity,
)
from pipeline.bridge_objective_redesign_simulation import (
    BridgeObjectiveRedesignSimulationError,
    run_bridge_objective_redesign_simulation,
)
from pipeline.cluster_inspection import (
    ClusterInspectionError,
    run_cluster_inspection,
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

def _print_artifact_values(path: Path, *key_paths: tuple[str, ...]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    for key_path in key_paths:
        value = payload
        for key in key_path:
            value = value[key]
        print(value)


def dispatch_command(args, parser, *, psycopg_module, compat_module=None) -> None:
    compat = compat_module or sys.modules[__name__]

    if args.command == "ml-blind-snapshot-review-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_blind_snapshot_review_worksheet import (
            MAX_ROWS as _BLIND_MAX_ROWS,
            MIN_ROWS as _BLIND_MIN_ROWS,
            MLBlindSnapshotReviewWorksheetError,
            run_ml_blind_snapshot_review_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        snap = (args.corpus_snapshot_version or "").strip()
        if not snap:
            parser.error("--corpus-snapshot-version is required and must be non-empty")
        emb = (args.embedding_version or "").strip()
        if not emb:
            parser.error("--embedding-version is required and must be non-empty")
        clv = (args.cluster_version or "").strip()
        if not clv:
            parser.error("--cluster-version is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _BLIND_MIN_ROWS or nrows > _BLIND_MAX_ROWS:
            parser.error(f"--rows must be between {_BLIND_MIN_ROWS} and {_BLIND_MAX_ROWS}")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_csv = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_blind_snapshot_review_worksheet_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                corpus_snapshot_version=snap,
                embedding_version=emb,
                cluster_version=clv,
                ranking_run_id=rid,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                markdown_output_path=out_md,
            )
        except MLBlindSnapshotReviewWorksheetError as e:
            print(f"ml-blind-snapshot-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-blind-snapshot-review-worksheet-v2":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_blind_snapshot_review_worksheet import (
            MAX_ROWS as _BLIND_MAX_ROWS,
            MIN_ROWS as _BLIND_MIN_ROWS,
            MLBlindSnapshotReviewWorksheetError,
        )
        from pipeline.ml_blind_snapshot_review_worksheet_v2 import run_ml_blind_snapshot_review_worksheet_v2_cli

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        snap = (args.corpus_snapshot_version or "").strip()
        if not snap:
            parser.error("--corpus-snapshot-version is required and must be non-empty")
        emb = (args.embedding_version or "").strip()
        if not emb:
            parser.error("--embedding-version is required and must be non-empty")
        clv = (args.cluster_version or "").strip()
        if not clv:
            parser.error("--cluster-version is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _BLIND_MIN_ROWS or nrows > _BLIND_MAX_ROWS:
            parser.error(f"--rows must be between {_BLIND_MIN_ROWS} and {_BLIND_MAX_ROWS}")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_csv = Path(args.output)
        out_ctx = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_blind_snapshot_review_worksheet_v2_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                corpus_snapshot_version=snap,
                embedding_version=emb,
                cluster_version=clv,
                ranking_run_id=rid,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                context_output_path=out_ctx,
                markdown_output_path=out_md,
            )
        except MLBlindSnapshotReviewWorksheetError as e:
            print(f"ml-blind-snapshot-review-worksheet-v2: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_ctx.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-hard-negative-review-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_hard_negative_review_worksheet import (
            MAX_ROWS as _HARD_NEG_MAX_ROWS,
            MIN_ROWS as _HARD_NEG_MIN_ROWS,
            MLHardNegativeReviewWorksheetError,
            run_ml_hard_negative_review_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        snap = (args.corpus_snapshot_version or "").strip()
        if not snap:
            parser.error("--corpus-snapshot-version is required and must be non-empty")
        emb = (args.embedding_version or "").strip()
        if not emb:
            parser.error("--embedding-version is required and must be non-empty")
        clv = (args.cluster_version or "").strip()
        if not clv:
            parser.error("--cluster-version is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _HARD_NEG_MIN_ROWS or nrows > _HARD_NEG_MAX_ROWS:
            parser.error(f"--rows must be between {_HARD_NEG_MIN_ROWS} and {_HARD_NEG_MAX_ROWS}")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_csv = Path(args.output)
        out_ctx = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_hard_negative_review_worksheet_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                corpus_snapshot_version=snap,
                embedding_version=emb,
                cluster_version=clv,
                ranking_run_id=rid,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                context_output_path=out_ctx,
                markdown_output_path=out_md,
            )
        except MLHardNegativeReviewWorksheetError as e:
            print(f"ml-hard-negative-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_ctx.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-bridge-negative-mining-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_bridge_negative_mining_worksheet import (
            MAX_ROWS as _BRIDGE_NEG_MAX_ROWS,
            MIN_ROWS as _BRIDGE_NEG_MIN_ROWS,
            MLBridgeNegativeMiningWorksheetError,
            run_ml_bridge_negative_mining_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _BRIDGE_NEG_MIN_ROWS or nrows > _BRIDGE_NEG_MAX_ROWS:
            parser.error(f"--rows must be between {_BRIDGE_NEG_MIN_ROWS} and {_BRIDGE_NEG_MAX_ROWS}")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_csv = Path(args.output)
        out_ctx = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_bridge_negative_mining_worksheet_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                ranking_run_id=rid,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                context_output_path=out_ctx,
                markdown_output_path=out_md,
            )
        except MLBridgeNegativeMiningWorksheetError as e:
            print(f"ml-bridge-negative-mining-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_ctx.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-bridge-top-ranked-validation-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_bridge_top_ranked_validation_worksheet import (
            MLBridgeTopRankedValidationWorksheetError,
            run_top_ranked_validation_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_csv = Path(args.output)
        out_ctx = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            result = run_top_ranked_validation_worksheet_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                top_n=int(args.top_n),
                contrastive_n=int(args.contrastive_n),
                contrastive_rank_max=int(args.contrastive_rank_max),
                csv_output_path=out_csv,
                context_output_path=out_ctx,
                markdown_output_path=out_md,
            )
        except MLBridgeTopRankedValidationWorksheetError as e:
            print(f"ml-bridge-top-ranked-validation-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_ctx.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(result.get("debug", {}).get("total_rows", 0)))
        return

    if args.command == "ml-external-near-miss-review-worksheet":
        from pipeline.ml_external_near_miss_review_worksheet import (
            MAX_ROWS as _EXT_MAX_ROWS,
            MIN_ROWS as _EXT_MIN_ROWS,
            MLExternalNearMissReviewWorksheetError,
            run_ml_external_near_miss_review_worksheet_cli,
        )

        snap = (args.corpus_snapshot_version or "").strip()
        if not snap:
            parser.error("--corpus-snapshot-version is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _EXT_MIN_ROWS or nrows > _EXT_MAX_ROWS:
            parser.error(f"--rows must be between {_EXT_MIN_ROWS} and {_EXT_MAX_ROWS}")
        candidate_plan = Path(args.source_snapshot_candidate_plan) if args.source_snapshot_candidate_plan else None
        out_csv = Path(args.output)
        out_ctx = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_external_near_miss_review_worksheet_cli(
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                corpus_snapshot_version=snap,
                candidate_plan_path=candidate_plan,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                context_output_path=out_ctx,
                markdown_output_path=out_md,
                mailto=args.mailto,
            )
        except MLExternalNearMissReviewWorksheetError as e:
            print(f"ml-external-near-miss-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_ctx.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-targeted-gap-review-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_targeted_gap_review_worksheet import (
            MLTargetedGapReviewWorksheetError,
            run_ml_targeted_gap_review_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        lim = int(args.limit)
        if lim < 1 or lim > 200:
            parser.error("--limit must be between 1 and 200")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        try:
            run_ml_targeted_gap_review_worksheet_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                family=str(args.family),
                target_gap=str(args.target_gap),
                output_csv=Path(args.output),
                markdown_output=Path(args.markdown_output),
                limit=lim,
            )
        except MLTargetedGapReviewWorksheetError as e:
            print(f"ml-targeted-gap-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "ml-contrastive-review-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_contrastive_review_worksheet import (
            MLContrastiveReviewWorksheetError,
            run_ml_contrastive_review_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        pf = int(args.per_family)
        if pf < 1 or pf > 200:
            parser.error("--per-family must be between 1 and 200")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        label_path = Path(args.label_dataset)
        try:
            run_ml_contrastive_review_worksheet_cli(
                database_url=dsn,
                label_dataset_path=label_path,
                ranking_run_id=rid,
                output_csv=Path(args.output),
                markdown_output=Path(args.markdown_output),
                per_family=pf,
            )
        except MLContrastiveReviewWorksheetError as e:
            print(f"ml-contrastive-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

    if args.command == "ml-label-dataset-v5-reviewer-blind-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v5_reviewer_blind_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v5"
        try:
            write_ml_label_dataset_v5_reviewer_blind_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v5-reviewer-blind-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v6-hard-negative-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v6_hard_negative_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v6"
        try:
            write_ml_label_dataset_v6_hard_negative_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v6-hard-negative-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v7-external-near-miss-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v7_external_near_miss_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v7"
        try:
            write_ml_label_dataset_v7_external_near_miss_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                conflict_policy_path=Path(args.conflict_policy),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v7-external-near-miss-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v8-transfer-gap-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v8_transfer_gap_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v8"
        try:
            write_ml_label_dataset_v8_transfer_gap_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                conflict_policy_path=Path(args.conflict_policy),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v8-transfer-gap-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v9-fresh-hybrid-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v9_fresh_hybrid_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v9"
        fresh_surface = Path(args.fresh_eval_surface) if args.fresh_eval_surface else None
        try:
            write_ml_label_dataset_v9_fresh_hybrid_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                conflict_policy_path=Path(args.conflict_policy),
                fresh_eval_surface_path=fresh_surface,
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v9-fresh-hybrid-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v10-fresh-positive-topup-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v10_fresh_positive_topup_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v10"
        fresh_surface = Path(args.fresh_eval_surface) if args.fresh_eval_surface else None
        try:
            write_ml_label_dataset_v10_fresh_positive_topup_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                conflict_policy_path=Path(args.conflict_policy),
                fresh_eval_surface_path=fresh_surface,
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v10-fresh-positive-topup-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v11-shadow-generalization-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v11_shadow_generalization_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v11"
        try:
            write_ml_label_dataset_v11_shadow_generalization_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                generalization_second_surface_path=Path(args.generalization_second_surface),
                conflict_policy_path=Path(args.conflict_policy),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v11-shadow-generalization-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v12-bridge-negative-mining-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v12_bridge_negative_mining_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v12"
        try:
            write_ml_label_dataset_v12_bridge_negative_mining_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                conflict_policy_path=Path(args.conflict_policy),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v12-bridge-negative-mining-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v13-bridge-top-ranked-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v13_bridge_top_ranked_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v13"
        try:
            write_ml_label_dataset_v13_bridge_top_ranked_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v13-bridge-top-ranked-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v14-bridge-shadow-pilot-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v14_bridge_shadow_pilot_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v14"
        try:
            write_ml_label_dataset_v14_bridge_shadow_pilot_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v14-bridge-shadow-pilot-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset":
        from pipeline.ml_label_dataset import write_ml_label_dataset

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        manual_dir = Path(args.manual_review_dir).resolve() if args.manual_review_dir else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or None
        write_ml_label_dataset(
            repo_root=repo_root,
            json_path=out_json,
            markdown_path=out_md,
            manual_review_dir=manual_dir,
            dataset_version=dver,
        )
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

    if args.command == "recommendation-review-worksheet":
        if args.limit < 1 or args.limit > 200:
            parser.error("--limit must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        if bool(args.bridge_eligible_only) and args.family != "bridge":
            parser.error("--bridge-eligible-only is only valid with --family bridge")
        try:
            compat.write_recommendation_review_worksheet(
                output_path=Path(args.output),
                database_url=args.database_url,
                ranking_run_id=rrid,
                family=args.family,
                limit=int(args.limit),
                bridge_eligible_only=bool(args.bridge_eligible_only),
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
        summary_paths: list[Path]
        explicit = [args.bridge_summary, args.emerging_summary, args.undercited_summary]
        if any(explicit):
            if not all(explicit):
                parser.error(
                    "--bridge-summary, --emerging-summary, and --undercited-summary must be provided together"
                )
            summary_paths = [
                Path(args.bridge_summary),
                Path(args.emerging_summary),
                Path(args.undercited_summary),
            ]
            if args.summary:
                parser.error("Use either repeated --summary or explicit family summary flags, not both")
        else:
            if not args.summary:
                parser.error("Provide at least one --summary or explicit family summary flags")
            summary_paths = [Path(x) for x in args.summary]
        try:
            run_recommendation_review_rollup(
                summary_paths=summary_paths,
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output)
                if args.markdown_output
                else None,
                bridge_diagnostics_path=Path(args.bridge_diagnostics)
                if args.bridge_diagnostics
                else None,
                bridge_worksheet_path=Path(args.bridge_worksheet)
                if args.bridge_worksheet
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
    if args.command == "bridge-eligibility-sensitivity":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        try:
            run_bridge_eligibility_sensitivity(
                ranking_run_id=rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeEligibilitySensitivityError as e:
            print(f"bridge-eligibility-sensitivity: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-objective-redesign-simulation":
        if int(args.k) != 20:
            parser.error("bridge-objective-redesign-simulation: --k must be 20")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        if args.repo_root:
            repo_root = Path(args.repo_root).resolve()
        else:
            cwd = Path.cwd().resolve()
            if (cwd / "docs" / "audit").is_dir():
                repo_root = cwd
            elif (cwd.parent / "docs" / "audit").is_dir():
                repo_root = cwd.parent
            elif (cwd.parent.parent / "docs" / "audit").is_dir():
                repo_root = cwd.parent.parent
            else:
                repo_root = cwd
        mr = repo_root / "docs" / "audit" / "manual-review"
        k = int(args.k)
        sens = Path(args.sensitivity_json).resolve() if args.sensitivity_json else mr / f"bridge_eligibility_sensitivity_{rrid}_top{k}.json"
        failp = (
            Path(args.failure_analysis_json).resolve()
            if args.failure_analysis_json
            else mr / f"bridge_eligibility_failure_analysis_{rrid}_top{k}.json"
        )
        csvp = (
            Path(args.bridge_worksheet_csv).resolve()
            if args.bridge_worksheet_csv
            else mr / f"bridge_eligible_{rrid}_top{k}.csv"
        )
        for p, label in ((sens, "sensitivity-json"), (failp, "failure-analysis-json"), (csvp, "bridge-worksheet-csv")):
            if not p.is_file():
                print(f"bridge-objective-redesign-simulation: missing {label}: {p}", file=sys.stderr)
                raise SystemExit(2)
        try:
            run_bridge_objective_redesign_simulation(
                ranking_run_id=rrid,
                k=k,
                sensitivity_json_path=sens,
                failure_analysis_json_path=failp,
                bridge_worksheet_csv_path=csvp,
                output_json_path=Path(args.output),
                markdown_path=Path(args.markdown_output),
                database_url=args.database_url,
            )
        except BridgeObjectiveRedesignSimulationError as e:
            print(f"bridge-objective-redesign-simulation: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-experiment-compare":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        baseline_rrid = (args.baseline_ranking_run_id or "").strip()
        experiment_rrid = (args.experiment_ranking_run_id or "").strip()
        if not baseline_rrid:
            parser.error("--baseline-ranking-run-id is required and must not be blank")
        if not experiment_rrid:
            parser.error("--experiment-ranking-run-id is required and must not be blank")
        try:
            run_bridge_weight_experiment_compare(
                baseline_ranking_run_id=baseline_rrid,
                experiment_ranking_run_id=experiment_rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
                baseline_bridge_worksheet_path=Path(args.baseline_bridge_worksheet)
                if args.baseline_bridge_worksheet
                else None,
            )
        except BridgeWeightExperimentCompareError as e:
            print(f"bridge-weight-experiment-compare: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-objective-experiment-compare":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        baseline_rrid = (args.baseline_ranking_run_id or "").strip()
        experiment_rrid = (args.experiment_ranking_run_id or "").strip()
        if not baseline_rrid:
            parser.error("--baseline-ranking-run-id is required and must not be blank")
        if not experiment_rrid:
            parser.error("--experiment-ranking-run-id is required and must not be blank")
        try:
            run_bridge_objective_experiment_compare(
                baseline_ranking_run_id=baseline_rrid,
                experiment_ranking_run_id=experiment_rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
                baseline_bridge_worksheet_path=Path(args.baseline_bridge_worksheet),
            )
        except BridgeObjectiveExperimentCompareError as e:
            print(f"bridge-objective-experiment-compare: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-experiment-delta-worksheet":
        baseline_rrid = (args.baseline_ranking_run_id or "").strip() or None
        experiment_rrid = (args.experiment_ranking_run_id or "").strip() or None
        try:
            rows = write_bridge_weight_experiment_delta_worksheet(
                comparison_path=Path(args.comparison),
                baseline_worksheet_path=Path(args.baseline_bridge_worksheet),
                diagnostics_path=Path(args.experiment_diagnostics),
                output_path=Path(args.output),
                database_url=args.database_url,
                baseline_ranking_run_id=baseline_rrid,
                experiment_ranking_run_id=experiment_rrid,
            )
        except BridgeWeightExperimentDeltaWorksheetError as e:
            print(f"bridge-weight-experiment-delta-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(len(rows))
        return
    if args.command == "bridge-objective-label-coverage":
        try:
            payload, rows = run_bridge_objective_label_coverage(
                comparison_path=Path(args.comparison),
                baseline_worksheet_path=Path(args.baseline_bridge_worksheet),
                prior_delta_worksheet_path=Path(args.prior_delta_worksheet),
                output_json_path=Path(args.output),
                output_markdown_path=Path(args.markdown_output),
                output_review_csv_path=Path(args.review_output),
            )
        except BridgeObjectiveLabelCoverageError as e:
            print(f"bridge-objective-label-coverage: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(Path(args.review_output).resolve(), file=sys.stderr)
        print(int(payload.get("summary", {}).get("truly_unlabeled_moved_in_count", len(rows))))
        return
    if args.command == "bridge-objective-labeled-outcome":
        try:
            run_bridge_objective_labeled_outcome(
                baseline_worksheet_path=Path(args.baseline_bridge_worksheet),
                prior_delta_worksheet_path=Path(args.prior_delta_worksheet),
                objective_delta_worksheet_path=Path(args.objective_delta_worksheet),
                objective_comparison_path=Path(args.objective_comparison),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
            )
        except BridgeObjectiveLabeledOutcomeError as e:
            print(f"bridge-objective-labeled-outcome: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-experiment-delta-summary":
        try:
            run_bridge_weight_experiment_delta_summary(
                input_path=Path(args.input),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
            )
        except BridgeWeightExperimentDeltaSummaryError as e:
            print(f"bridge-weight-experiment-delta-summary: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-response-rollup":
        try:
            run_bridge_weight_response_rollup(
                baseline_review_rollup_path=Path(args.baseline_review_rollup),
                compare_zero_vs_w005_path=Path(args.compare_zero_vs_w005),
                delta_review_summary_path=Path(args.delta_review_summary),
                compare_w005_vs_w010_path=Path(args.compare_w005_vs_w010),
                compare_zero_vs_w010_path=Path(args.compare_zero_vs_w010),
                labeled_baseline_bridge_worksheet_path=Path(args.labeled_baseline_bridge_worksheet),
                delta_review_csv_path=Path(args.delta_review_csv),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeWeightResponseRollupError as e:
            print(f"bridge-weight-response-rollup: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-labeled-outcome":
        try:
            run_bridge_weight_labeled_outcome(
                baseline_worksheet_path=Path(args.baseline_bridge_worksheet),
                delta_worksheet_path=Path(args.delta_review_csv),
                response_rollup_path=Path(args.response_rollup),
                compare_zero_vs_w005_path=Path(args.compare_zero_vs_w005),
                compare_w005_vs_w010_path=Path(args.compare_w005_vs_w010),
                compare_zero_vs_w010_path=Path(args.compare_zero_vs_w010),
                diagnostics_rank_zero_path=Path(args.diagnostics_zero),
                diagnostics_rank_w005_path=Path(args.diagnostics_w005),
                diagnostics_rank_w010_path=Path(args.diagnostics_w010),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeWeightLabeledOutcomeError as e:
            print(f"bridge-weight-labeled-outcome: {e}", file=sys.stderr)
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
            bridge_eligibility_mode=args.bridge_eligibility_mode,
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
        return

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
        return
