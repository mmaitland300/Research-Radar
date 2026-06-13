from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_ml_review_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

    return False
