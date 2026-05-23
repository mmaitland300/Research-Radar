"""Tests for second-surface learned-probability coverage plan."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_shadow_scorer_second_surface_learned_probability_coverage_plan import (
    EXPECTED_CANDIDATE_POOL_WORK_COUNT,
    EXPECTED_CANDIDATE_SHA,
    EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
    EXPECTED_EMBEDDING_VERSION,
    EXPECTED_FAMILY,
    EXPECTED_NEGATIVE_COUNT,
    EXPECTED_POSITIVE_COUNT,
    EXPECTED_RANKING_RUN_ID,
    MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError,
    PLAN_VERSION,
    build_ml_shadow_scorer_second_surface_learned_probability_coverage_plan_payload,
    markdown_from_ml_shadow_scorer_second_surface_learned_probability_coverage_plan,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _discovery_payload(*, status: str = "selected_needs_learned_probability_coverage") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_second_surface",
            "surface_version": "ml-shadow-scorer-v1-generalization-second-surface-v1",
            "source_label_dataset_version": "ml-label-dataset-v11",
        },
        "discovery_summary": {
            "status": status,
            "recommended_next_stage": "create_second_surface_learned_probability_coverage_plan_v1",
        },
        "selected_second_surface": {
            "ranking_run_id": EXPECTED_RANKING_RUN_ID,
            "family": EXPECTED_FAMILY,
            "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
            "confirmatory_metric_eligible_work_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
            "embedding_version": EXPECTED_EMBEDDING_VERSION,
            "final_score_coverage_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "learned_probability_coverage_count": 0,
            "missing_learned_probability_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        },
        "threshold_check": {
            "minimum_confirmatory_labeled_work_count": {
                "observed": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
                "threshold": 100,
                "passed": True,
            },
            "minimum_confirmatory_positive_work_count": {
                "observed": EXPECTED_POSITIVE_COUNT,
                "threshold": 50,
                "passed": True,
            },
            "minimum_confirmatory_negative_work_count": {
                "observed": EXPECTED_NEGATIVE_COUNT,
                "threshold": 20,
                "passed": True,
            },
            "minimum_distinct_negative_work_count": {
                "observed": EXPECTED_NEGATIVE_COUNT,
                "threshold": 20,
                "passed": True,
            },
            "minimum_confirmatory_label_coverage_rate": {
                "observed": 1.0,
                "threshold": 0.6,
                "passed": True,
            },
            "unresolved_label_conflicts": {"observed": 0, "threshold": 0, "passed": True},
            "learned_probability_coverage": {
                "observed": 0,
                "threshold": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
                "passed": False,
            },
        },
        "label_coverage": {
            "work_level": {
                "confirmatory_candidate_work_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
                "confirmatory_labeled_work_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
                "confirmatory_positive_work_count": EXPECTED_POSITIVE_COUNT,
                "confirmatory_negative_work_count": EXPECTED_NEGATIVE_COUNT,
                "distinct_negative_work_count": EXPECTED_NEGATIVE_COUNT,
                "conflicting_target_work_group_count": 0,
                "label_coverage_rate": 1.0,
            }
        },
        "learned_probability_coverage": {
            "approved_upstream_probability_probe": {
                "probe_status": "not_found",
                "learned_probability_coverage_count": 0,
                "full_coverage": False,
                "source_artifact_path": None,
            },
            "embedding_coverage_probe": {
                "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
                "embedding_coverage_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
                "embedding_version": EXPECTED_EMBEDDING_VERSION,
                "full_embedding_coverage": True,
            },
            "learned_probability_coverage_count": 0,
            "missing_learned_probability_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "scorer_execution_used": False,
        },
        "readiness_for_generalization_audit": {
            "candidate_source_selected": True,
            "ready_for_generalization_audit_execution": False,
            "status": status,
        },
        "recommended_next_stage": "create_second_surface_learned_probability_coverage_plan_v1",
    }


def _label_dataset_payload(*, version: str = "ml-label-dataset-v11", thresholds_passed: bool = True) -> dict:
    return {
        "dataset_version": version,
        "metadata": {
            "dataset_version": version,
            "shadow_generalization_second_surface_v1_ingest": {
                "row_count_appended": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
                "ranking_run_id": EXPECTED_RANKING_RUN_ID,
                "family": EXPECTED_FAMILY,
                "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
                "confirmatory_metric_eligible_work_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
                "labeled_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
                "positive_count": EXPECTED_POSITIVE_COUNT,
                "negative_count": EXPECTED_NEGATIVE_COUNT,
                "good_count": 45,
                "acceptable_count": 49,
                "miss_count": 63,
                "irrelevant_count": 11,
                "distinct_negative_work_count": EXPECTED_NEGATIVE_COUNT,
                "label_thresholds_passed": thresholds_passed,
            },
        },
        "rows": [],
    }


def _embeddings_payload(*, embedded: int = EXPECTED_CANDIDATE_POOL_WORK_COUNT) -> dict:
    missing = EXPECTED_CANDIDATE_POOL_WORK_COUNT - embedded
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_snapshot_embeddings",
            "artifact_version": "ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
            "embedding_version": EXPECTED_EMBEDDING_VERSION,
        },
        "coverage": {
            "snapshot_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "embedded_work_count": embedded,
            "missing_embedding_count": missing,
        },
        "embedding_result": {"full_snapshot_embedding_coverage": missing == 0},
    }


def _scorer_payload(*, artifact_type: str = "ml_offline_audit_embedding_scorer", version: str = "ml-offline-audit-embedding-scorer-v2") -> dict:
    return {"metadata": {"artifact_type": artifact_type, "scorer_version": version}}


def _generalization_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_plan",
            "plan_version": "ml-shadow-scorer-v1-generalization-audit-v1",
        },
        "generalization_audit_plan_defined": True,
        "runtime_implementation_authorized": False,
    }


def _online_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_policy",
            "policy_version": "ml-shadow-scorer-v1-online-shadow-policy",
        },
        "runtime_implementation_authorized": False,
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    discovery: dict | None = None,
    labels: dict | None = None,
    embeddings: dict | None = None,
    scorer: dict | None = None,
    plan: dict | None = None,
    policy: dict | None = None,
) -> dict[str, Path]:
    return {
        "generalization_second_surface_path": _write_json(
            tmp_path, "generalization-second-surface.json", discovery or _discovery_payload()
        ),
        "label_dataset_path": _write_json(tmp_path, "ml-label-dataset-v11.json", labels or _label_dataset_payload()),
        "second_snapshot_embeddings_path": _write_json(
            tmp_path, "second-snapshot-embeddings.json", embeddings or _embeddings_payload()
        ),
        "offline_audit_embedding_scorer_path": _write_json(
            tmp_path, "offline-audit-embedding-scorer.json", scorer or _scorer_payload()
        ),
        "generalization_audit_plan_path": _write_json(
            tmp_path, "generalization-audit-plan.json", plan or _generalization_plan_payload()
        ),
        "online_shadow_policy_path": _write_json(tmp_path, "online-shadow-policy.json", policy or _online_policy_payload()),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_second_surface_learned_probability_coverage_plan_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-23T00:00:00Z",
    )


def test_happy_path_writes_contract_fields_and_blockers(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    contract = payload["learned_probability_coverage_contract"]
    blockers = payload["shadow_and_production_blockers"]

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_surface_learned_probability_coverage_plan"
    assert payload["metadata"]["plan_version"] == PLAN_VERSION
    assert payload["learned_probability_coverage_plan_defined"] is True
    assert contract["approved_scorer"] == "ml-offline-audit-embedding-scorer-v2"
    assert contract["approved_embedding_version"] == EXPECTED_EMBEDDING_VERSION
    assert contract["must_not_refit"] is True
    assert contract["must_not_regenerate_embeddings"] is True
    assert contract["must_not_use_v11_labels_as_scorer_features"] is True
    assert contract["target_coverage"]["candidate_pool_work_count"] == 528
    assert blockers["missing_second_surface_learned_probability_coverage"] is True
    assert blockers["runtime_implementation_authorized"] is False
    assert payload["recommended_next_stage"] == "apply_second_surface_learned_probability_coverage_v1"


def test_rejects_wrong_discovery_status(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="status"):
        _build(tmp_path, discovery=_discovery_payload(status="selected_needs_labels"))


def test_rejects_wrong_ranking_run_candidate_sha_or_pool_count(tmp_path: Path) -> None:
    discovery = _discovery_payload()
    discovery["selected_second_surface"]["ranking_run_id"] = "rank-other"
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="ranking_run_id"):
        _build(tmp_path / "run", discovery=discovery)

    discovery = _discovery_payload()
    discovery["selected_second_surface"]["candidate_pool_work_set_sha256"] = "bad"
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="candidate_pool_work_set_sha256"):
        _build(tmp_path / "sha", discovery=discovery)

    discovery = _discovery_payload()
    discovery["selected_second_surface"]["candidate_pool_work_count"] = 527
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="candidate_pool_work_count"):
        _build(tmp_path / "pool", discovery=discovery)


def test_rejects_incomplete_embedding_coverage(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="embedded_work_count"):
        _build(tmp_path, embeddings=_embeddings_payload(embedded=527))


def test_rejects_wrong_scorer_metadata(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="artifact_type"):
        _build(tmp_path / "type", scorer=_scorer_payload(artifact_type="wrong"))

    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="scorer_version"):
        _build(tmp_path / "version", scorer=_scorer_payload(version="ml-offline-audit-embedding-scorer-v1"))


def test_rejects_label_dataset_version_threshold_or_count_mismatch(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="version"):
        _build(tmp_path / "version", labels=_label_dataset_payload(version="ml-label-dataset-v10"))

    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="label_thresholds_passed"):
        _build(tmp_path / "threshold", labels=_label_dataset_payload(thresholds_passed=False))

    labels = _label_dataset_payload()
    labels["metadata"]["shadow_generalization_second_surface_v1_ingest"]["positive_count"] = 93
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityCoveragePlanError, match="positive_count"):
        _build(tmp_path / "counts", labels=labels)


def test_markdown_includes_contract_and_blocker_semantics_note(tmp_path: Path) -> None:
    markdown = markdown_from_ml_shadow_scorer_second_surface_learned_probability_coverage_plan(_build(tmp_path))

    assert "Learned-Probability Contract" in markdown
    assert "Approved scorer: `ml-offline-audit-embedding-scorer-v2`" in markdown
    assert "Blocker Semantics Note" in markdown
    assert "does not mean no surface was selected" in markdown


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "plan.json"
    out_md = tmp_path / "plan.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-second-surface-learned-probability-coverage-plan",
        "--generalization-second-surface",
        str(paths["generalization_second_surface_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--second-snapshot-embeddings",
        str(paths["second_snapshot_embeddings_path"]),
        "--offline-audit-embedding-scorer",
        str(paths["offline_audit_embedding_scorer_path"]),
        "--generalization-audit-plan",
        str(paths["generalization_audit_plan_path"]),
        "--online-shadow-policy",
        str(paths["online_shadow_policy_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["evidence_summary"]["learned_probability_coverage"]["learned_probability_coverage_count"] == 0
    assert "Learned-Probability Contract" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_surface_learned_probability_coverage_plan.py"
    ).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in module_source.splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    for forbidden in ("psycopg", "postgres", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-second-surface-learned-probability-coverage-plan"')
    end = cli_source.index('"ml-shadow-scorer-second-hybrid-candidate-plan"', start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
