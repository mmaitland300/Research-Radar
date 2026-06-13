from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_review_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

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
        return True

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
        return True
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
        return True

    return False
