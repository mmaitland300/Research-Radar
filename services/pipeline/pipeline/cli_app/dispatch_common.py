from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
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
from pipeline.source_resolution import resolve_all_sources, slug_to_openalex_id_map


@dataclass(frozen=True)
class DispatchContext:
    parser: argparse.ArgumentParser
    psycopg_module: object
    compat: object


def _print_artifact_values(path: Path, *key_paths: tuple[str, ...]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    for key_path in key_paths:
        value = payload
        for key in key_path:
            value = value[key]
        print(value)


__all__ = [name for name in globals() if not name.startswith("__")]
