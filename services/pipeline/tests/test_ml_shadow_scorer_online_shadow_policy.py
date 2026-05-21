"""Tests for ml-shadow-scorer-v1 online shadow policy drafting."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_shadow_scorer_online_shadow_policy import (
    FEATURE_FLAG,
    MLShadowScorerOnlineShadowPolicyError,
    POLICY_VERSION,
    build_ml_shadow_scorer_online_shadow_policy_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
FRESH_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"


def _audit_output_gates_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_audit_output_gates",
            "gates_version": "ml-shadow-scorer-v1-audit-output-gates",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "shadow_audit_output_gates_passed": True,
        "audit_output_complete": True,
        "validation_replay_exact": True,
        "offline_audit_output_ready": True,
        "shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": "draft_online_shadow_execution_policy_v1",
        "overall_outcomes": {
            "shadow_audit_output_gates_passed": True,
            "validation_replay_exact": True,
            "offline_audit_output_ready": True,
            "shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "recommended_next_stage": "draft_online_shadow_execution_policy_v1",
        },
    }


def _audit_output_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_audit_output",
            "artifact_version": "ml-shadow-scorer-v1-audit-output",
            "candidate_pool_work_set_sha256": FRESH_SHA,
            "shadow_execution_enabled": False,
            "production_default_changed": False,
            "api_web_changed": False,
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
            "embedding_version": "fresh-hybrid-text-embedding-v1",
        },
        "execution_summary": {
            "status": "succeeded",
            "output_row_count": 358,
            "candidate_pool_size": 358,
        },
        "execution_verification": {
            "output_matches_validation_replay": True,
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
    }


def _readiness_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_execution_readiness_gates",
            "gates_version": "ml-shadow-scorer-v1-execution-readiness-gates",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "shadow_scorer_execution_readiness_passed": True,
        "overall_outcomes": {"shadow_scorer_execution_readiness_passed": True},
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
    audit_output_gates: dict | None = None,
    audit_output: dict | None = None,
    readiness: dict | None = None,
    spec: dict | None = None,
    validation: dict | None = None,
    production_plan: dict | None = None,
) -> dict[str, Path]:
    return {
        "shadow_scorer_audit_output_gates_path": _write_json(
            tmp_path, "audit-output-gates.json", audit_output_gates or _audit_output_gates_payload()
        ),
        "shadow_scorer_audit_output_path": _write_json(
            tmp_path, "audit-output.json", audit_output or _audit_output_payload()
        ),
        "shadow_scorer_execution_readiness_gates_path": _write_json(
            tmp_path, "readiness.json", readiness or _readiness_payload()
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
    return build_ml_shadow_scorer_online_shadow_policy_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def _set_path(payload: dict, dotted_path: str, value: object) -> None:
    current: dict = payload
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        current = current[part]
    current[parts[-1]] = value


def test_happy_path_creates_policy_from_passed_audit_output_gates(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["policy_version"] == POLICY_VERSION
    assert payload["online_shadow_execution_policy_defined"] is True
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["runtime_implementation_authorized"] is False
    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False
    assert payload["recommended_next_stage"] == "draft_ml_shadow_scorer_v1_generalization_audit_v1"


def test_rejects_if_audit_output_gates_did_not_pass(tmp_path: Path) -> None:
    gates = _audit_output_gates_payload()
    gates["shadow_audit_output_gates_passed"] = False
    gates["overall_outcomes"]["shadow_audit_output_gates_passed"] = False

    with pytest.raises(MLShadowScorerOnlineShadowPolicyError, match="shadow_audit_output_gates_passed"):
        _build(tmp_path, audit_output_gates=gates)


def test_rejects_if_gates_recommended_next_stage_is_wrong(tmp_path: Path) -> None:
    gates = _audit_output_gates_payload()
    gates["recommended_next_stage"] = "implement_online_shadow_runtime_disabled_by_default"
    gates["overall_outcomes"]["recommended_next_stage"] = "implement_online_shadow_runtime_disabled_by_default"

    with pytest.raises(MLShadowScorerOnlineShadowPolicyError, match="recommended_next_stage"):
        _build(tmp_path, audit_output_gates=gates)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("shadow_execution_enabled", True, "shadow_execution_enabled"),
        ("production_default_allowed", True, "production_default_allowed"),
    ],
)
def test_rejects_if_shadow_or_prod_enabled_in_gates(
    tmp_path: Path, path: str, value: object, message: str
) -> None:
    gates = _audit_output_gates_payload()
    gates[path] = value
    gates["overall_outcomes"][path] = value

    with pytest.raises(MLShadowScorerOnlineShadowPolicyError, match=message):
        _build(tmp_path, audit_output_gates=gates)


@pytest.mark.parametrize(
    ("artifact_name", "path"),
    [
        ("audit_output", "metadata.candidate_pool_work_set_sha256"),
        ("audit_output", "execution_verification.candidate_pool_work_set_sha256"),
        ("readiness", "metadata.candidate_pool_work_set_sha256"),
        ("spec", "metadata.candidate_pool_work_set_sha256"),
        ("validation", "metadata.candidate_pool_work_set_sha256"),
        ("validation", "candidate_eval_coverage.candidate_pool_work_set_sha256"),
    ],
)
def test_rejects_candidate_sha_mismatch_across_required_paths(
    tmp_path: Path, artifact_name: str, path: str
) -> None:
    payloads = {
        "audit_output": _audit_output_payload(),
        "readiness": _readiness_payload(),
        "spec": _spec_payload(),
        "validation": _validation_payload(),
    }
    _set_path(payloads[artifact_name], path, "bad")

    with pytest.raises(MLShadowScorerOnlineShadowPolicyError, match="candidate_pool_work_set_sha256"):
        _build(tmp_path, **payloads)


def test_policy_requires_feature_flag_default_off(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    runtime_policy = payload["runtime_isolation_policy"]
    assert runtime_policy["feature_flag"] == FEATURE_FLAG
    assert runtime_policy["feature_flag_default"] == "off"
    assert runtime_policy["feature_flag_default_off"] is True


def test_policy_forbids_embedding_generation_and_skips_incomplete_coverage(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    data_contract = payload["data_dependency_contract"]

    assert data_contract["embedding_generation_at_shadow_time_allowed"] is False
    assert data_contract["learned_scorer_refit_allowed"] is False
    assert data_contract["learned_probability_creation_by_online_shadow_runtime_allowed"] is False
    assert data_contract["skip_on_incomplete_coverage"] is True
    assert "does not require learned probability to live in embedding storage" in data_contract[
        "learned_probability_storage_requirement"
    ]


def test_policy_includes_snapshot_bound_validation_scope(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    scope = payload["validation_snapshot_scope"]

    assert scope["ranking_run_id"] == "rank-9f4b2a2084"
    assert scope["family"] == "emerging"
    assert scope["corpus_snapshot_version"] == "source-snapshot-fresh-hybrid-v1-20260518"
    assert scope["embedding_version"] == "fresh-hybrid-text-embedding-v1"
    assert scope["candidate_pool_work_set_sha256"] == FRESH_SHA
    assert scope["formula_generalization_beyond_validated_surface_asserted"] is False


def test_policy_includes_generalization_requirement_before_runtime(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    requirement = payload["generalization_requirement_before_runtime"]

    assert requirement["second_surface_generalization_audit_must_pass"] is True
    assert requirement["runtime_implementation_authorized_by_this_policy"] is False
    assert requirement["required_next_artifacts_in_order"] == [
        "draft_ml_shadow_scorer_v1_generalization_audit_v1",
        "audit_ml_shadow_scorer_v1_on_second_fresh_surface",
        "ml-shadow-scorer-v1-generalization-audit-gates",
    ]


def test_recommended_next_stage_is_generalization_plan_not_runtime(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["recommended_next_stage"] == "draft_ml_shadow_scorer_v1_generalization_audit_v1"
    assert payload["recommended_next_stage"] != "implement_online_shadow_runtime_disabled_by_default"


def test_blockers_after_policy_keep_runtime_and_prod_blocked(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["missing_online_shadow_execution_policy"] is False
    assert blockers["missing_generalization_audit_on_second_surface"] is True
    assert blockers["missing_online_shadow_implementation_disabled_by_default"] is True
    assert blockers["missing_shadow_runtime_isolation_verification"] is True
    assert blockers["missing_production_readiness_authorization"] is True
    assert blockers["online_shadow_execution_enabled"] is False
    assert blockers["runtime_implementation_authorized"] is False
    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False


def test_rejects_overall_outcome_disagreement(tmp_path: Path) -> None:
    gates = _audit_output_gates_payload()
    gates["overall_outcomes"]["offline_audit_output_ready"] = False

    with pytest.raises(MLShadowScorerOnlineShadowPolicyError, match="offline_audit_output_ready"):
        _build(tmp_path, audit_output_gates=gates)


def test_rejects_unblocked_production_readiness_plan(tmp_path: Path) -> None:
    production_plan = _production_plan_payload()
    production_plan["production_default_authorized"] = True

    with pytest.raises(MLShadowScorerOnlineShadowPolicyError, match="production readiness plan"):
        _build(tmp_path, production_plan=production_plan)


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "online-shadow-policy.json"
    out_md = tmp_path / "online-shadow-policy.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-policy",
        "--shadow-scorer-audit-output-gates",
        str(paths["shadow_scorer_audit_output_gates_path"]),
        "--shadow-scorer-audit-output",
        str(paths["shadow_scorer_audit_output_path"]),
        "--shadow-scorer-execution-readiness-gates",
        str(paths["shadow_scorer_execution_readiness_gates_path"]),
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
    assert data["metadata"]["policy_version"] == POLICY_VERSION
    assert data["online_shadow_execution_policy_defined"] is True
    assert "Online Shadow Policy" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_policy.py").read_text(
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
    command_index = cli_source.index("ml-shadow-scorer-online-shadow-policy")
    next_command_index = cli_source.index("ml-fresh-eval-labeling-plan-hybrid", command_index)
    command_block = cli_source[command_index:next_command_index]
    assert "--database-url" not in command_block
