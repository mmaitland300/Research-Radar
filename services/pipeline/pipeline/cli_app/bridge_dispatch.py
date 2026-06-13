from __future__ import annotations

from pipeline.cli_app.dispatch_common import *


def handle_bridge_commands(args, ctx: DispatchContext) -> bool:
    parser = ctx.parser
    psycopg_module = ctx.psycopg_module
    compat = ctx.compat

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
        return True

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
        return True
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
        return True
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
        return True
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
        return True
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
        return True
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
        return True
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
        return True
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
        return True
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
        return True
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
        return True
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
        return True

    return False
