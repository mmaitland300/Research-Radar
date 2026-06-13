from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_ml_label_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

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
        return True

    return False
