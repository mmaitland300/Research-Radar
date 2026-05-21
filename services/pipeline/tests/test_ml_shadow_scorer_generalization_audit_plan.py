"""Tests for ml-shadow-scorer-v1 generalization audit plan."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_shadow_scorer_generalization_audit_plan import (
    MLShadowScorerGeneralizationAuditPlanError,
    PLAN_VERSION,
    build_ml_shadow_scorer_generalization_audit_plan_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
FRESH_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"


def _online_shadow_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_policy",
            "policy_version": "ml-shadow-scorer-v1-online-shadow-policy",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "online_shadow_execution_policy_defined": True,
        "online_shadow_execution_enabled": False,
        "runtime_implementation_authorized": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": "draft_ml_shadow_scorer_v1_generalization_audit_v1",
        "generalization_requirement_before_runtime": {"required_before_runtime_implementation": True},
        "shadow_and_production_blockers": {"missing_generalization_audit_on_second_surface": True},
    }


def _audit_output_gates_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_audit_output_gates",
            "gates_version": "ml-shadow-scorer-v1-audit-output-gates",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "shadow_audit_output_gates_passed": True,
        "offline_audit_output_ready": True,
        "validation_replay_exact": True,
        "overall_outcomes": {
            "shadow_audit_output_gates_passed": True,
            "offline_audit_output_ready": True,
            "validation_replay_exact": True,
        },
    }


def _spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_spec",
            "spec_version": "ml-shadow-scorer-v1-spec",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "scoring_formula": {
            "formula_id": "hybrid_rank_mean_50_50",
            "components": [
                {"name": "final_score_rank_pct", "source": "rank_pct(final_score)", "weight": 0.5},
                {
                    "name": "audit_embedding_probability_rank_pct",
                    "source": "rank_pct(audit_embedding_probability_work)",
                    "weight": 0.5,
                },
            ],
        },
    }


def _fresh_surface_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        },
        "gate_linkage": {
            "material_lift_thresholds": {
                "delta_roc_auc_gte": 0.03,
                "or_delta_average_precision_gte": 0.02,
            }
        },
        "label_policy": {
            "minimum_confirmatory_label_thresholds": {
                "minimum_candidate_work_count": 100,
                "minimum_confirmatory_label_coverage_rate": 0.6,
                "minimum_confirmatory_labeled_work_count": 100,
                "minimum_confirmatory_negative_work_count": 20,
                "minimum_confirmatory_positive_work_count": 50,
                "minimum_distinct_negative_work_count": 20,
            }
        },
    }


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
        },
        "production_default_authorized": False,
        "targets": {"good_or_acceptable": {"production_eligible": False}},
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    policy: dict | None = None,
    audit_output_gates: dict | None = None,
    spec: dict | None = None,
    fresh_surface_policy: dict | None = None,
    production_plan: dict | None = None,
) -> dict[str, Path]:
    return {
        "online_shadow_policy_path": _write_json(tmp_path, "online-policy.json", policy or _online_shadow_policy_payload()),
        "shadow_scorer_audit_output_gates_path": _write_json(
            tmp_path, "audit-output-gates.json", audit_output_gates or _audit_output_gates_payload()
        ),
        "shadow_scorer_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "fresh_surface_policy_path": _write_json(
            tmp_path, "fresh-surface-policy.json", fresh_surface_policy or _fresh_surface_policy_payload()
        ),
        "production_readiness_plan_path": _write_json(
            tmp_path, "production-plan.json", production_plan or _production_plan_payload()
        ),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_generalization_audit_plan_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def test_happy_path_creates_generalization_plan_from_policy(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["plan_version"] == PLAN_VERSION
    assert payload["generalization_audit_plan_defined"] is True
    assert payload["generalization_audit_executed"] is False
    assert payload["runtime_implementation_authorized"] is False
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["recommended_next_stage"] == "materialize_or_select_second_fresh_surface_for_shadow_generalization_v1"


def test_rejects_if_runtime_implementation_authorized_true(tmp_path: Path) -> None:
    policy = _online_shadow_policy_payload()
    policy["runtime_implementation_authorized"] = True

    with pytest.raises(MLShadowScorerGeneralizationAuditPlanError, match="runtime_implementation_authorized"):
        _build(tmp_path, policy=policy)


def test_rejects_if_policy_recommended_next_stage_wrong(tmp_path: Path) -> None:
    policy = _online_shadow_policy_payload()
    policy["recommended_next_stage"] = "implement_online_shadow_runtime_disabled_by_default"

    with pytest.raises(MLShadowScorerGeneralizationAuditPlanError, match="recommended_next_stage"):
        _build(tmp_path, policy=policy)


def test_rejects_if_audit_output_gates_did_not_pass(tmp_path: Path) -> None:
    gates = _audit_output_gates_payload()
    gates["shadow_audit_output_gates_passed"] = False
    gates["overall_outcomes"]["shadow_audit_output_gates_passed"] = False

    with pytest.raises(MLShadowScorerGeneralizationAuditPlanError, match="shadow_audit_output_gates_passed"):
        _build(tmp_path, audit_output_gates=gates)


def test_requires_second_surface_sha_and_ranking_run_to_differ_from_validated_surface(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    requirements = payload["second_surface_requirements"]

    assert requirements["candidate_pool_work_set_sha256_must_differ_from"] == FRESH_SHA
    assert requirements["ranking_run_id_must_differ_from"] == "rank-9f4b2a2084"
    assert requirements["must_not_reuse_materialized_surface"] == "ml-fresh-eval-surface-hybrid-v1"


def test_requires_label_thresholds_including_distinct_negatives(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    labels = payload["label_requirements"]

    assert labels["minimum_confirmatory_labeled_work_count"] == 100
    assert labels["minimum_confirmatory_positive_work_count"] == 50
    assert labels["minimum_confirmatory_negative_work_count"] == 20
    assert labels["minimum_distinct_negative_work_count"] == 20
    assert labels["minimum_confirmatory_label_coverage_rate"] == 0.6


def test_requires_material_lift_thresholds_from_policy(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    lift = payload["metric_requirements"]["material_lift"]

    assert lift["delta_roc_auc_gte"] == 0.03
    assert lift["or_delta_average_precision_gte"] == 0.02


def test_rejects_missing_material_lift_thresholds(tmp_path: Path) -> None:
    policy = _fresh_surface_policy_payload()
    policy["gate_linkage"]["material_lift_thresholds"] = {}

    with pytest.raises(MLShadowScorerGeneralizationAuditPlanError, match="material_lift_thresholds"):
        _build(tmp_path, fresh_surface_policy=policy)


def test_forbids_embedding_generation_refit_and_tuning_in_replay_requirements(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    replay = payload["scorer_replay_requirements"]

    assert replay["embedding_generation_inside_audit_allowed"] is False
    assert replay["learned_scorer_refit_allowed"] is False
    assert replay["learned_probability_creation_inside_audit_allowed"] is False
    assert replay["weight_tuning_allowed"] is False


def test_blockers_after_plan_keep_generalization_runtime_and_prod_blocked(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["missing_generalization_audit_plan_v1"] is False
    assert blockers["missing_generalization_audit_on_second_surface"] is True
    assert blockers["missing_generalization_audit_gates"] is True
    assert blockers["missing_online_shadow_implementation_disabled_by_default"] is True
    assert blockers["missing_shadow_runtime_isolation_verification"] is True
    assert blockers["missing_production_readiness_authorization"] is True
    assert blockers["runtime_implementation_authorized"] is False
    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False


def test_planned_commit_sequence_documents_no_runtime_until_gates(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    sequence = payload["planned_commit_sequence"]

    assert sequence[0]["this_commit"] is True
    assert sequence[-1]["commit"] == "feat(eval): implement online shadow runtime disabled by default"
    assert sequence[-1]["only_after_step_4_passes"] is True


def test_rejects_unblocked_production_readiness_plan(tmp_path: Path) -> None:
    production_plan = _production_plan_payload()
    production_plan["production_default_authorized"] = True

    with pytest.raises(MLShadowScorerGeneralizationAuditPlanError, match="production readiness plan"):
        _build(tmp_path, production_plan=production_plan)


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "generalization-plan.json"
    out_md = tmp_path / "generalization-plan.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-generalization-audit-plan",
        "--online-shadow-policy",
        str(paths["online_shadow_policy_path"]),
        "--shadow-scorer-audit-output-gates",
        str(paths["shadow_scorer_audit_output_gates_path"]),
        "--shadow-scorer-spec",
        str(paths["shadow_scorer_spec_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["metadata"]["plan_version"] == PLAN_VERSION
    assert data["generalization_audit_plan_defined"] is True
    assert "Generalization Audit Plan" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_generalization_audit_plan.py").read_text(
        encoding="utf-8"
    ).lower()
    test_source = Path(__file__).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    for forbidden in ("psycopg", "postgres", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8").lower()
    command_index = cli_source.index("ml-shadow-scorer-generalization-audit-plan")
    next_command_index = cli_source.index("ml-fresh-eval-labeling-plan-hybrid", command_index)
    command_block = cli_source[command_index:next_command_index]
    assert "--database-url" not in command_block
