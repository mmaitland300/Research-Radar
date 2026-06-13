from __future__ import annotations


def register_bridge_operational_parsers(subparsers) -> None:
    bridge_readiness_parser = subparsers.add_parser(
        "bridge-experiment-readiness",
        help="Join recommendation review rollup with paper_scores top-k overlap for bridge weight go/no-go",
    )
    bridge_readiness_parser.add_argument(
        "--rollup",
        required=True,
        help="Path to rank-level recommendation review rollup JSON",
    )
    bridge_readiness_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (must match rollup provenance and ranking_runs row)",
    )
    bridge_readiness_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_readiness_parser.add_argument(
        "--output",
        required=True,
        help="Path to write readiness JSON",
    )
    bridge_readiness_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write readiness Markdown",
    )
    bridge_readiness_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    bridge_diag_parser = subparsers.add_parser(
        "bridge-signal-diagnostics",
        help="Read-only bridge signal diagnostics for one explicit ranking_run_id (paper_scores + ranking_runs)",
    )
    bridge_weight_compare_parser = subparsers.add_parser(
        "bridge-weight-experiment-compare",
        help="Read-only baseline vs experiment comparison for a small bridge-weight run",
    )
    bridge_weight_delta_parser = subparsers.add_parser(
        "bridge-weight-experiment-delta-worksheet",
        help="Read-only CSV worksheet for unlabeled moved-in eligible bridge experiment rows",
    )
    bridge_objective_label_coverage_parser = subparsers.add_parser(
        "bridge-objective-label-coverage",
        help="Read-only objective-experiment label coverage correction and one-row review scaffold",
    )
    bridge_objective_labeled_outcome_parser = subparsers.add_parser(
        "bridge-objective-labeled-outcome",
        help="Read-only labeled outcome rollup for objective experiment (baseline + prior delta + one-row label)",
    )
    bridge_weight_delta_summary_parser = subparsers.add_parser(
        "bridge-weight-experiment-delta-summary",
        help="Validate and summarize a completed bridge-weight delta review worksheet CSV",
    )
    bridge_sensitivity_parser = subparsers.add_parser(
        "bridge-eligibility-sensitivity",
        help="Read-only threshold sweep for bridge eligibility distinctness on one explicit ranking_run_id",
    )
    return {
        "bridge_diag_parser": bridge_diag_parser,
        "bridge_weight_compare_parser": bridge_weight_compare_parser,
        "bridge_weight_delta_parser": bridge_weight_delta_parser,
        "bridge_objective_label_coverage_parser": bridge_objective_label_coverage_parser,
        "bridge_objective_labeled_outcome_parser": bridge_objective_labeled_outcome_parser,
        "bridge_weight_delta_summary_parser": bridge_weight_delta_summary_parser,
        "bridge_sensitivity_parser": bridge_sensitivity_parser,
    }


def register_bridge_operational_arguments(bridge_parsers) -> None:
    bridge_diag_parser = bridge_parsers["bridge_diag_parser"]
    bridge_sensitivity_parser = bridge_parsers["bridge_sensitivity_parser"]
    bridge_diag_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (no latest fallback)",
    )
    bridge_diag_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_diag_parser.add_argument(
        "--output",
        required=True,
        help="Path to write diagnostics JSON",
    )
    bridge_diag_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write diagnostics Markdown",
    )
    bridge_diag_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_sensitivity_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (no latest fallback)",
    )
    bridge_sensitivity_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size used for overlap diagnostics (default 20)",
    )
    bridge_sensitivity_parser.add_argument(
        "--output",
        required=True,
        help="Path to write sensitivity JSON",
    )
    bridge_sensitivity_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write sensitivity Markdown",
    )
    bridge_sensitivity_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )


def register_bridge_analysis_parsers(subparsers, bridge_parsers) -> None:
    bridge_weight_compare_parser = bridge_parsers["bridge_weight_compare_parser"]
    bridge_weight_delta_parser = bridge_parsers["bridge_weight_delta_parser"]
    bridge_objective_label_coverage_parser = bridge_parsers["bridge_objective_label_coverage_parser"]
    bridge_objective_labeled_outcome_parser = bridge_parsers["bridge_objective_labeled_outcome_parser"]
    bridge_weight_delta_summary_parser = bridge_parsers["bridge_weight_delta_summary_parser"]
    bridge_objective_sim_parser = subparsers.add_parser(
        "bridge-objective-redesign-simulation",
        help="Read-only simulation of alternative bridge objectives (SELECT-only DB; no ranking writes)",
    )
    bridge_objective_sim_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (e.g. zero-weight baseline run)",
    )
    bridge_objective_sim_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Emerging / eligible overlap window (must be 20 for this simulation)",
    )
    bridge_objective_sim_parser.add_argument(
        "--output",
        required=True,
        help="Path to write simulation JSON",
    )
    bridge_objective_sim_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write simulation Markdown",
    )
    bridge_objective_sim_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root for default audit paths (default: cwd, or parent if cwd is services/pipeline)",
    )
    bridge_objective_sim_parser.add_argument(
        "--sensitivity-json",
        default=None,
        help="bridge_eligibility_sensitivity JSON (default: docs/audit/manual-review/... under repo root)",
    )
    bridge_objective_sim_parser.add_argument(
        "--failure-analysis-json",
        default=None,
        help="bridge_eligibility_failure_analysis JSON (default under docs/audit/manual-review/)",
    )
    bridge_objective_sim_parser.add_argument(
        "--bridge-worksheet-csv",
        default=None,
        help="Labeled bridge eligible top-20 CSV (default: bridge_eligible_<run>_top20.csv)",
    )
    bridge_objective_sim_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_weight_compare_parser.add_argument(
        "--baseline-ranking-run-id",
        required=True,
        help="Baseline ranking_run_id (typically zero bridge weight)",
    )
    bridge_weight_compare_parser.add_argument(
        "--experiment-ranking-run-id",
        required=True,
        help="Experiment ranking_run_id (small positive bridge weight)",
    )
    bridge_weight_compare_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_weight_compare_parser.add_argument(
        "--output",
        required=True,
        help="Path to write comparison JSON",
    )
    bridge_weight_compare_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write comparison Markdown",
    )
    bridge_weight_compare_parser.add_argument(
        "--baseline-bridge-worksheet",
        "--labeled-bridge-worksheet",
        dest="baseline_bridge_worksheet",
        default=None,
        help=(
            "Optional labeled bridge worksheet CSV path for already-reviewed baseline/delta rows. "
            "Default: docs/audit/manual-review/bridge_eligible_<baseline_run_id>_top20.csv"
        ),
    )
    bridge_weight_compare_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_objective_compare_parser = subparsers.add_parser(
        "bridge-objective-experiment-compare",
        help="Read-only baseline vs experiment when only bridge_eligibility_mode differs (same bridge weight)",
    )
    bridge_objective_compare_parser.add_argument(
        "--baseline-ranking-run-id",
        required=True,
        help="Baseline ranking_run_id",
    )
    bridge_objective_compare_parser.add_argument(
        "--experiment-ranking-run-id",
        required=True,
        help="Experiment ranking_run_id (different bridge_eligibility_mode)",
    )
    bridge_objective_compare_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_objective_compare_parser.add_argument(
        "--output",
        required=True,
        help="Path to write comparison JSON",
    )
    bridge_objective_compare_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write comparison Markdown",
    )
    bridge_objective_compare_parser.add_argument(
        "--baseline-bridge-worksheet",
        "--labeled-bridge-worksheet",
        dest="baseline_bridge_worksheet",
        required=True,
        help="Baseline labeled bridge eligible top-k CSV (for new-unlabeled detection)",
    )
    bridge_objective_compare_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_weight_delta_parser.add_argument(
        "--comparison",
        required=True,
        help="Bridge weight or objective experiment comparison JSON artifact",
    )
    bridge_weight_delta_parser.add_argument(
        "--baseline-bridge-worksheet",
        required=True,
        help="Baseline labeled bridge eligible worksheet CSV",
    )
    bridge_weight_delta_parser.add_argument(
        "--experiment-diagnostics",
        required=True,
        help="Experiment bridge signal diagnostics JSON artifact",
    )
    bridge_weight_delta_parser.add_argument(
        "--output",
        required=True,
        help="Path to write delta review worksheet CSV",
    )
    bridge_weight_delta_parser.add_argument(
        "--baseline-ranking-run-id",
        default=None,
        help="Optional guard: expected baseline ranking_run_id",
    )
    bridge_weight_delta_parser.add_argument(
        "--experiment-ranking-run-id",
        default=None,
        help="Optional guard: expected experiment ranking_run_id",
    )
    bridge_weight_delta_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--comparison",
        required=True,
        help="Objective experiment comparison JSON artifact",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--baseline-bridge-worksheet",
        required=True,
        help="Baseline labeled bridge eligible worksheet CSV",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--prior-delta-worksheet",
        required=True,
        help="Previously labeled bridge-weight delta worksheet CSV",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--output",
        required=True,
        help="Path to write label coverage correction JSON",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write label coverage correction Markdown",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--review-output",
        required=True,
        help="Path to write one-row unlabeled review worksheet CSV",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--baseline-bridge-worksheet",
        required=True,
        help="Baseline labeled bridge eligible worksheet CSV",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--prior-delta-worksheet",
        required=True,
        help="Prior bridge-weight delta labeled worksheet CSV",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--objective-delta-worksheet",
        required=True,
        help="Objective one-row labeled worksheet CSV",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--objective-comparison",
        required=True,
        help="Objective experiment comparison JSON artifact",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--output",
        required=True,
        help="Path to write objective labeled outcome JSON",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write objective labeled outcome Markdown",
    )
    bridge_weight_delta_summary_parser.add_argument(
        "--input",
        required=True,
        help="Completed delta review worksheet CSV path",
    )
    bridge_weight_delta_summary_parser.add_argument(
        "--output",
        required=True,
        help="Path to write delta summary JSON",
    )
    bridge_weight_delta_summary_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write delta summary Markdown",
    )
    bridge_weight_response_rollup_parser = subparsers.add_parser(
        "bridge-weight-response-rollup",
        help="Synthesize zero / w005 / w010 bridge-weight experiments into one JSON (+ optional Markdown) artifact",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--baseline-review-rollup",
        required=True,
        help="Path to baseline rank review rollup JSON (zero-weight run)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--compare-zero-vs-w005",
        required=True,
        help="Path to bridge_weight_experiment compare JSON (zero vs 0.05)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--delta-review-summary",
        required=True,
        help="Path to completed delta review summary JSON (0.05 experiment)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--compare-w005-vs-w010",
        required=True,
        help="Path to compare JSON (0.05 vs 0.10)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--compare-zero-vs-w010",
        required=True,
        help="Path to compare JSON (zero vs 0.10)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--labeled-baseline-bridge-worksheet",
        required=True,
        help="CSV path with baseline bridge eligible labels (paper_id column)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--delta-review-csv",
        required=True,
        help="Completed delta review worksheet CSV (paper_id column)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--output",
        required=True,
        help="Path to write bridge weight response rollup JSON",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write rollup Markdown",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL for label coverage check (default: DATABASE_URL or PG* env)",
    )
    bridge_weight_labeled_outcome_parser = subparsers.add_parser(
        "bridge-weight-labeled-outcome",
        help="Eligible-bridge top-20 label quality by weight using baseline + delta worksheets (read-only)",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--baseline-bridge-worksheet",
        required=True,
        help="Baseline bridge eligible top-20 labeled CSV (rank-ee2ba6c816)",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--delta-review-csv",
        required=True,
        help="Completed 0.05 delta review CSV",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--response-rollup",
        required=True,
        help="bridge_weight_response_rollup JSON (movement + stack)",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--compare-zero-vs-w005",
        required=True,
        help="Compare JSON zero vs 0.05",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--compare-w005-vs-w010",
        required=True,
        help="Compare JSON 0.05 vs 0.10",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--compare-zero-vs-w010",
        required=True,
        help="Compare JSON zero vs 0.10",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--diagnostics-zero",
        required=True,
        help="bridge_signal_diagnostics JSON for rank-ee2ba6c816",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--diagnostics-w005",
        required=True,
        help="bridge_signal_diagnostics JSON for rank-bc1123e00c",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--diagnostics-w010",
        required=True,
        help="bridge_signal_diagnostics JSON for rank-9a02c81d40",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--output",
        required=True,
        help="Path to write labeled outcome JSON",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write labeled outcome Markdown",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL to resolve work_id→paper_id for eligible head (default: DATABASE_URL or PG* env)",
    )
