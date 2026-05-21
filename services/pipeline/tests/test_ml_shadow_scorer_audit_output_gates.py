"""Tests for ml-shadow-scorer-v1 audit output gates."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_shadow_scorer_audit_output_gates import (
    GATES_VERSION,
    MLShadowScorerAuditOutputGatesError,
    build_ml_shadow_scorer_audit_output_gates_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
FRESH_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"


def _fake_inputs() -> list[dict[str, str]]:
    return [{"name": "x", "path": "docs/audit/x.json", "sha256": "a" * 64}]


def _rows(count: int = 358) -> list[dict]:
    rows: list[dict] = []
    for index in range(1, count + 1):
        rows.append(
            {
                "shadow_rank": index,
                "canonical_openalex_work_id": f"W{index:06d}",
                "title": f"Work {index}",
                "year": 2020 + (index % 6),
                "ranking_run_id": "rank-9f4b2a2084",
                "family": "emerging",
                "final_score": float(400 - index),
                "audit_embedding_probability_work": float(index) / 358.0,
                "final_score_rank_pct": 1.0 - ((index - 1) / 357.0),
                "audit_embedding_probability_rank_pct": (index - 1) / 357.0,
                "ml_shadow_scorer_v1_score": 0.5,
                "heuristic_rank": index,
                "confirmatory_metric_eligible": index <= 143,
                "label_any_positive": None,
                "label_any_positive_not_used_for_scoring": True,
            }
        )
    return rows


def _audit_output_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_audit_output",
            "artifact_version": "ml-shadow-scorer-v1-audit-output",
            "inputs": _fake_inputs(),
            "shadow_execution_enabled": False,
            "production_default_changed": False,
            "api_web_changed": False,
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "formula_id": "hybrid_rank_mean_50_50",
            "scorer_id": "ml-shadow-scorer-v1",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "source_contract": {
            "required_shadow_output_isolation_contract": {"isolated_audit_shadow_outputs_only": True},
            "required_observability_contract": {"requirements": ["component coverage counts"]},
            "audit_file_satisfies_isolation_contract_for_offline_path_only": True,
        },
        "execution_summary": {
            "status": "succeeded",
            "candidate_pool_size": 358,
            "output_row_count": 358,
            "learned_probability_coverage_count": 358,
            "missing_learned_probability_count": 0,
        },
        "coverage_summary": {
            "confirmatory_metric_eligible_count": 143,
            "component_coverage": {"candidate_pool_size": 358},
        },
        "execution_verification": {
            "output_matches_validation_replay": True,
            "candidate_pool_work_set_sha256": FRESH_SHA,
            "max_abs_score_delta": 0.0,
            "max_abs_rank_pct_delta": 0.0,
            "mismatched_work_count": 0,
        },
        "score_distribution": {"ml_shadow_scorer_v1_score": {"count": 358}},
        "rank_displacement_summary": {"count": 358},
        "top_k_overlap_summary": {"k_5": {"overlap_count": 1}},
        "observability_summary": {"shadow_output_completeness": {"complete": True}},
        "top_k_preview": [{"shadow_rank": 1, "canonical_openalex_work_id": "W000001"}],
        "shadow_output_rows": _rows(),
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "missing_ml_shadow_scorer_v1_audit_output_gates": True,
        },
        "recommended_next_stage": "draft_ml_shadow_scorer_v1_audit_output_gates",
    }


def _readiness_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_execution_readiness_gates",
            "gates_version": "ml-shadow-scorer-v1-execution-readiness-gates",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "shadow_scorer_execution_readiness_passed": True,
        "shadow_audit_execution_allowed": True,
        "shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "overall_outcomes": {
            "shadow_scorer_execution_readiness_passed": True,
            "shadow_audit_execution_allowed": True,
            "shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
    }


def _implementation_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_implementation",
            "implementation_version": "ml-shadow-scorer-v1-implementation",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "implementation_status": {
            "implemented": True,
            "disabled_by_default": True,
            "implementation_matches_spec": True,
            "implementation_matches_validation_replay": True,
            "missing_ml_shadow_scorer_v1_implementation": False,
        },
        "audit_replay_summary": {"candidate_pool_work_set_sha256": FRESH_SHA},
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


def _validation_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_validation_on_fresh_surface",
            "validation_version": "ml-hybrid-validation-on-fresh-surface-v1",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "candidate_eval_coverage": {"candidate_pool_work_set_sha256": FRESH_SHA},
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
    audit_output: dict | None = None,
    readiness: dict | None = None,
    implementation: dict | None = None,
    spec: dict | None = None,
    validation: dict | None = None,
    production_plan: dict | None = None,
) -> dict[str, Path]:
    return {
        "shadow_scorer_audit_output_path": _write_json(tmp_path, "audit-output.json", audit_output or _audit_output_payload()),
        "shadow_scorer_execution_readiness_gates_path": _write_json(
            tmp_path, "readiness.json", readiness or _readiness_payload()
        ),
        "shadow_scorer_implementation_path": _write_json(
            tmp_path, "implementation.json", implementation or _implementation_payload()
        ),
        "shadow_scorer_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "hybrid_validation_on_fresh_surface_path": _write_json(
            tmp_path, "validation.json", validation or _validation_payload()
        ),
        "production_readiness_plan_path": _write_json(
            tmp_path, "production-plan.json", production_plan or _production_plan_payload()
        ),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_audit_output_gates_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def _gate(payload: dict, gate_id: str) -> dict:
    return next(item for item in payload["gate_results"] if item["gate_id"] == gate_id)


def _set_path(payload: dict, dotted_path: str, value: object) -> None:
    current: dict = payload
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        current = current[part]
    current[parts[-1]] = value


def test_happy_path_passes_all_gates(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["shadow_audit_output_gates_passed"] is True
    assert payload["offline_audit_output_ready"] is True
    assert payload["recommended_next_stage"] == "draft_online_shadow_execution_policy_v1"
    assert all(gate["status"] == "pass" for gate in payload["gate_results"])


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("execution_summary.output_row_count", 357, "output_row_count"),
        ("execution_summary.learned_probability_coverage_count", 357, "learned_probability_coverage_count"),
        ("execution_verification.output_matches_validation_replay", False, "output_matches_validation_replay"),
        ("execution_verification.max_abs_score_delta", 0.1, "max_abs_score_delta"),
    ],
)
def test_precheck_failures_for_counts_coverage_replay_and_deltas(
    tmp_path: Path, path: str, value: object, message: str
) -> None:
    audit_output = _audit_output_payload()
    _set_path(audit_output, path, value)

    with pytest.raises(MLShadowScorerAuditOutputGatesError, match=message):
        _build(tmp_path, audit_output=audit_output)


def test_row_schema_failure_raises(tmp_path: Path) -> None:
    audit_output = _audit_output_payload()
    del audit_output["shadow_output_rows"][0]["final_score"]

    with pytest.raises(MLShadowScorerAuditOutputGatesError, match="row schema"):
        _build(tmp_path, audit_output=audit_output)


def test_shadow_enabled_fails_fast(tmp_path: Path) -> None:
    audit_output = _audit_output_payload()
    audit_output["metadata"]["shadow_execution_enabled"] = True

    with pytest.raises(MLShadowScorerAuditOutputGatesError, match="shadow_execution_enabled"):
        _build(tmp_path, audit_output=audit_output)


def test_production_allowed_fails_fast(tmp_path: Path) -> None:
    audit_output = _audit_output_payload()
    audit_output["shadow_and_production_blockers"]["production_default_allowed"] = True

    with pytest.raises(MLShadowScorerAuditOutputGatesError, match="production_default_allowed"):
        _build(tmp_path, audit_output=audit_output)


@pytest.mark.parametrize(
    ("artifact_name", "path"),
    [
        ("audit_output", "metadata.candidate_pool_work_set_sha256"),
        ("audit_output", "execution_verification.candidate_pool_work_set_sha256"),
        ("readiness", "metadata.candidate_pool_work_set_sha256"),
        ("implementation", "metadata.candidate_pool_work_set_sha256"),
        ("implementation", "audit_replay_summary.candidate_pool_work_set_sha256"),
        ("spec", "metadata.candidate_pool_work_set_sha256"),
        ("validation", "metadata.candidate_pool_work_set_sha256"),
        ("validation", "candidate_eval_coverage.candidate_pool_work_set_sha256"),
    ],
)
def test_sha_mismatch_fails_for_all_linkage_paths(tmp_path: Path, artifact_name: str, path: str) -> None:
    payloads = {
        "audit_output": _audit_output_payload(),
        "readiness": _readiness_payload(),
        "implementation": _implementation_payload(),
        "spec": _spec_payload(),
        "validation": _validation_payload(),
    }
    _set_path(payloads[artifact_name], path, "bad")

    with pytest.raises(MLShadowScorerAuditOutputGatesError, match="candidate_pool_work_set_sha256"):
        _build(tmp_path, **payloads)


def test_observability_empty_makes_gate_fail(tmp_path: Path) -> None:
    audit_output = _audit_output_payload()
    audit_output["observability_summary"] = {}

    payload = _build(tmp_path, audit_output=audit_output)

    assert _gate(payload, "G07_observability_populated")["status"] == "fail"
    assert payload["shadow_audit_output_gates_passed"] is False
    assert payload["recommended_next_stage"] == "repair_ml_shadow_scorer_v1_audit_output"


def test_blockers_update_correctly_on_pass(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["missing_ml_shadow_scorer_v1_spec"] is False
    assert blockers["missing_ml_shadow_scorer_v1_implementation"] is False
    assert blockers["missing_shadow_execution_readiness_gates"] is False
    assert blockers["missing_shadow_output_isolation_check"] is False
    assert blockers["missing_ml_shadow_scorer_v1_audit_output_artifact"] is False
    assert blockers["missing_ml_shadow_scorer_v1_audit_output_gates"] is False
    assert blockers["missing_online_shadow_execution_policy"] is True
    assert blockers["missing_shadow_runtime_isolation_verification"] is True
    assert blockers["missing_production_readiness_authorization"] is True
    assert blockers["shadow_execution_enabled"] is False
    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "audit-output-gates.json"
    out_md = tmp_path / "audit-output-gates.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-audit-output-gates",
        "--shadow-scorer-audit-output",
        str(paths["shadow_scorer_audit_output_path"]),
        "--shadow-scorer-execution-readiness-gates",
        str(paths["shadow_scorer_execution_readiness_gates_path"]),
        "--shadow-scorer-implementation",
        str(paths["shadow_scorer_implementation_path"]),
        "--shadow-scorer-spec",
        str(paths["shadow_scorer_spec_path"]),
        "--hybrid-validation-on-fresh-surface",
        str(paths["hybrid_validation_on_fresh_surface_path"]),
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
    assert data["shadow_audit_output_gates_passed"] is True
    assert "ML Shadow Scorer v1 Audit Output Gates" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_audit_output_gates.py").read_text(
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
    command_index = cli_source.index("ml-shadow-scorer-audit-output-gates")
    next_command_index = cli_source.index("ml-fresh-eval-labeling-plan-hybrid", command_index)
    command_block = cli_source[command_index:next_command_index]
    assert "--database-url" not in command_block
