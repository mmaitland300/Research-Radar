"""Tests for ml-shadow-scorer-v1 execution readiness gates."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

from pipeline.ml_shadow_scorer_execution_readiness_gates import (
    GATES_VERSION,
    build_ml_shadow_scorer_execution_readiness_gates_payload,
)


FRESH_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"


def _implementation_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_implementation",
            "implementation_version": "ml-shadow-scorer-v1-implementation",
            "shadow_execution_enabled": False,
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "recommended_next_stage": "draft_ml_shadow_scorer_v1_execution_readiness_gates",
        "implementation_status": {
            "implemented": True,
            "disabled_by_default": True,
            "implementation_matches_spec": True,
            "implementation_matches_validation_replay": True,
            "missing_ml_shadow_scorer_v1_implementation": False,
            "candidate_pool_size": 358,
            "learned_probability_coverage_count": 358,
            "missing_learned_probability_count": 0,
        },
        "audit_replay_summary": {
            "candidate_pool_work_set_sha256": FRESH_SHA,
            "mismatched_work_count": 0,
            "max_abs_score_delta": 0.0,
            "max_abs_rank_pct_delta": 0.0,
            "replay_tolerance": 1e-12,
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "production_default_changed": False,
            "api_web_changed": False,
            "missing_ml_shadow_scorer_v1_implementation": False,
            "missing_shadow_execution_readiness_gates": True,
            "missing_shadow_output_isolation_check": True,
        },
    }


def _spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_spec",
            "spec_version": "ml-shadow-scorer-v1-spec",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "spec_ready_for_implementation": True,
        "recommended_next_stage": "implement_ml_shadow_scorer_v1_disabled_by_default",
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "scoring_formula": {
            "formula_id": "hybrid_rank_mean_50_50",
            "scoring_formula_literal": "score = 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
            "components": [
                {"name": "final_score_rank_pct", "source": "rank_pct(final_score)", "weight": 0.5},
                {
                    "name": "audit_embedding_probability_rank_pct",
                    "source": "rank_pct(audit_embedding_probability_work)",
                    "weight": 0.5,
                },
            ],
        },
        "forbidden_inputs": [
            "relevance_label",
            "good_or_acceptable",
            "label_any_positive",
            "reviewer_notes",
        ],
        "execution_policy": {
            "future_implementation_write_scope": "isolated shadow/audit outputs only",
            "production_ranking_changes_allowed": False,
            "api_web_changes_allowed": False,
            "bridge_default_changes_allowed": False,
            "production_default_promotion_authorized": False,
        },
        "observability_requirements": [
            "component coverage counts",
            "missing learned probability count",
            "score distribution for final_score",
            "score distribution for audit_embedding_probability_work",
            "score distribution for hybrid shadow score",
            "top-k overlap with heuristic final_score",
            "rank displacement summary",
            "family-level counts",
            "shadow output completeness",
            "error counters if implemented online",
            "latency counters if implemented online",
        ],
    }


def _gates_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_validation_metric_gates",
            "gates_version": "ml-hybrid-validation-metric-gates-v1",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "recommended_next_stage": "draft_ml_shadow_scorer_v1_spec",
        "confirmatory_validation_passed": True,
        "primary_hybrid_material_lift_passed": True,
        "fresh_surface_hybrid_validation_passed": True,
        "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "comparison_summary": {
            "candidate_eval_coverage": {
                "candidate_pool_work_set_sha256": FRESH_SHA,
            },
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
    implementation: dict | None = None,
    spec: dict | None = None,
    gates: dict | None = None,
    production_plan: dict | None = None,
) -> dict[str, Path]:
    return {
        "shadow_scorer_implementation_path": _write_json(
            tmp_path, "implementation.json", implementation or _implementation_payload()
        ),
        "shadow_scorer_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "hybrid_validation_metric_gates_path": _write_json(tmp_path, "gates.json", gates or _gates_payload()),
        "production_readiness_plan_path": _write_json(
            tmp_path, "production_plan.json", production_plan or _production_plan_payload()
        ),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_execution_readiness_gates_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def _gate(payload: dict, gate_id: str) -> dict:
    return next(item for item in payload["gate_results"] if item["gate_id"] == gate_id)


def test_happy_path_all_gates_pass(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["shadow_scorer_execution_readiness_passed"] is True
    assert payload["implementation_exact_replay_passed"] is True
    assert payload["shadow_audit_execution_allowed"] is True
    assert payload["recommended_next_stage"] == "implement_ml_shadow_scorer_v1_audit_output_artifact"
    assert all(gate["status"] == "pass" for gate in payload["gate_results"])


def test_fails_for_replay_mismatch(tmp_path: Path) -> None:
    implementation = _implementation_payload()
    implementation["audit_replay_summary"]["mismatched_work_count"] = 1

    payload = _build(tmp_path, implementation=implementation)

    assert _gate(payload, "G03_implementation_exact_replay")["status"] == "fail"
    assert payload["shadow_scorer_execution_readiness_passed"] is False
    assert payload["shadow_audit_execution_allowed"] is False


def test_fails_for_incomplete_learned_probability_coverage(tmp_path: Path) -> None:
    implementation = _implementation_payload()
    implementation["implementation_status"]["learned_probability_coverage_count"] = 357
    implementation["implementation_status"]["missing_learned_probability_count"] = 1

    payload = _build(tmp_path, implementation=implementation)

    assert _gate(payload, "G04_component_coverage")["status"] == "fail"
    assert payload["shadow_scorer_execution_readiness_passed"] is False


def test_fails_if_shadow_execution_enabled_true(tmp_path: Path) -> None:
    implementation = _implementation_payload()
    implementation["metadata"]["shadow_execution_enabled"] = True

    payload = _build(tmp_path, implementation=implementation)

    assert _gate(payload, "G05_disabled_by_default")["status"] == "fail"
    assert payload["shadow_execution_enabled"] is False
    assert payload["shadow_scorer_execution_readiness_passed"] is False


def test_fails_if_production_default_allowed_true(tmp_path: Path) -> None:
    gates = _gates_payload()
    gates["production_default_allowed"] = True

    payload = _build(tmp_path, gates=gates)

    assert _gate(payload, "G02_confirmatory_validation_passed")["status"] == "fail"
    assert payload["production_default_allowed"] is False
    assert payload["shadow_scorer_execution_readiness_passed"] is False


def test_fails_if_confirmatory_validation_passed_false(tmp_path: Path) -> None:
    gates = _gates_payload()
    gates["confirmatory_validation_passed"] = False

    payload = _build(tmp_path, gates=gates)

    assert _gate(payload, "G02_confirmatory_validation_passed")["status"] == "fail"
    assert payload["shadow_scorer_execution_readiness_passed"] is False


def test_fails_on_candidate_pool_sha_mismatch(tmp_path: Path) -> None:
    spec = _spec_payload()
    spec["metadata"]["candidate_pool_work_set_sha256"] = "bad"

    payload = _build(tmp_path, spec=spec)

    assert _gate(payload, "G01_evidence_chain_complete")["status"] == "fail"
    assert payload["prechecks"]["candidate_sha_checks"]["all_match_expected"] is False
    assert payload["shadow_scorer_execution_readiness_passed"] is False


def test_isolation_contract_present(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    contract = payload["required_shadow_output_isolation_contract"]

    assert contract["isolated_audit_shadow_outputs_only"] is True
    assert contract["no_production_ranking_table_or_config_writes"] is True
    assert {"run_id", "scorer_version", "formula_id", "input_hashes"}.issubset(contract["required_fields"])


def test_observability_contract_copied_from_spec(tmp_path: Path) -> None:
    spec = _spec_payload()
    spec["observability_requirements"].append("custom completeness drilldown")

    payload = _build(tmp_path, spec=spec)

    assert "custom completeness drilldown" in payload["required_observability_contract"]["requirements"]
    assert payload["required_observability_contract"]["all_required_topics_present"] is True


def test_blockers_update_correctly_on_pass(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["missing_ml_shadow_scorer_v1_spec"] is False
    assert blockers["missing_ml_shadow_scorer_v1_implementation"] is False
    assert blockers["missing_shadow_execution_readiness_gates"] is False
    assert blockers["missing_shadow_output_isolation_check"] is False
    assert blockers["missing_ml_shadow_scorer_v1_audit_output_artifact"] is True
    assert blockers["confirmatory_validation_not_complete"] is False
    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "readiness.json"
    out_md = tmp_path / "readiness.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-execution-readiness-gates",
        "--shadow-scorer-implementation",
        str(paths["shadow_scorer_implementation_path"]),
        "--shadow-scorer-spec",
        str(paths["shadow_scorer_spec_path"]),
        "--hybrid-validation-metric-gates",
        str(paths["hybrid_validation_metric_gates_path"]),
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
    assert data["metadata"]["gates_version"] == GATES_VERSION
    assert data["shadow_scorer_execution_readiness_passed"] is True
    assert "ML Shadow Scorer v1 Execution Readiness Gates" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    module_source = Path("pipeline/ml_shadow_scorer_execution_readiness_gates.py").read_text(encoding="utf-8").lower()
    test_source = Path("tests/test_ml_shadow_scorer_execution_readiness_gates.py").read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    for forbidden in ("psycopg", "postgres", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = Path("pipeline/cli.py").read_text(encoding="utf-8").lower()
    command_index = cli_source.index("ml-shadow-scorer-execution-readiness-gates")
    next_command_index = cli_source.index("ml-fresh-eval-labeling-plan-hybrid", command_index)
    command_block = cli_source[command_index:next_command_index]
    assert "--database-url" not in command_block
