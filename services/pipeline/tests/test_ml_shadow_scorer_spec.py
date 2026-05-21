"""Tests for ml-shadow-scorer-v1 spec drafting."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_shadow_scorer_spec import (
    SPEC_VERSION,
    MLShadowScorerSpecError,
    build_ml_shadow_scorer_spec_payload,
    markdown_from_ml_shadow_scorer_spec,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


FRESH_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"


def _gates_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_validation_metric_gates",
            "gates_version": "ml-hybrid-validation-metric-gates-v1",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "confirmatory_validation_passed": True,
        "fresh_surface_hybrid_validation_passed": True,
        "primary_hybrid_material_lift_passed": True,
        "recommended_next_stage": "draft_ml_shadow_scorer_v1_spec",
        "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "shadow_blockers": [
            "missing_ml_shadow_scorer_v1_implementation",
            "production_default_blocked",
            "no_production_model_artifact",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "missing_ml_shadow_scorer_v1_implementation": True,
            "confirmatory_validation_not_complete": False,
        },
        "comparison_summary": {
            "candidate_eval_coverage": {
                "candidate_pool_work_count": 358,
                "candidate_pool_work_set_sha256": FRESH_SHA,
                "confirmatory_metric_work_count": 143,
                "confirmatory_positive_work_count": 54,
                "confirmatory_negative_work_count": 89,
            },
            "primary_hybrid_arm": {
                "arm_id": "hybrid_rank_mean_50_50",
                "deltas_vs_heuristic": {
                    "delta_roc_auc": 0.08510195588847269,
                    "delta_average_precision": 0.21226464709880377,
                },
            },
        },
    }


def _validation_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_validation_on_fresh_surface",
            "validation_version": "ml-hybrid-validation-on-fresh-surface-v1",
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
            "embedding_version": "fresh-hybrid-text-embedding-v1",
            "candidate_pool_work_set_sha256": FRESH_SHA,
        },
        "validation_scope": {
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
            "embedding_version": "fresh-hybrid-text-embedding-v1",
        },
        "confirmatory_decision_inputs": {
            "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
            "secondary_reporting_arm": "hybrid_rank_mean_25_75_heuristic",
        },
    }


def _surface_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
            "status": "materialized_ready",
        },
        "ready_for_hybrid_validation_scoring": True,
        "candidate_source": {
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
        },
        "candidate_pool": {
            "candidate_work_count": 358,
            "candidate_work_set_sha256": FRESH_SHA,
        },
    }


def _policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        },
        "frozen_hybrid_arms": {
            "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
            "secondary_reporting_arm": "hybrid_rank_mean_25_75_heuristic",
        },
    }


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
        },
        "targets": {"good_or_acceptable": {"production_eligible": False}},
        "production_default_authorized": False,
    }


def _scorer_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_audit_embedding_scorer",
            "scorer_version": "ml-offline-audit-embedding-scorer-v2",
            "fit_mode": "holdout_bound_train_only",
        },
        "policy_compliance": {"eval_works_excluded_from_fit": True},
    }


def _hybrid_spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_offline_experiment_spec",
            "spec_version": "ml-hybrid-scorer-offline-experiment-v1-spec",
        }
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    gates: dict | None = None,
    validation: dict | None = None,
    surface: dict | None = None,
    policy: dict | None = None,
    plan: dict | None = None,
    scorer: dict | None = None,
    hybrid_spec: dict | None = None,
) -> dict[str, Path]:
    return {
        "hybrid_validation_metric_gates_path": _write_json(tmp_path, "gates.json", gates or _gates_payload()),
        "hybrid_validation_on_fresh_surface_path": _write_json(
            tmp_path, "validation.json", validation or _validation_payload()
        ),
        "fresh_eval_surface_path": _write_json(tmp_path, "surface.json", surface or _surface_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
        "production_readiness_plan_path": _write_json(tmp_path, "plan.json", plan or _production_plan_payload()),
        "audit_embedding_scorer_export_path": _write_json(tmp_path, "scorer.json", scorer or _scorer_payload()),
        "hybrid_experiment_spec_path": _write_json(tmp_path, "hybrid-spec.json", hybrid_spec or _hybrid_spec_payload()),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_spec_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def test_happy_path_creates_spec_from_passed_gates(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_spec"
    assert payload["metadata"]["spec_version"] == SPEC_VERSION
    assert payload["scorer_contract"]["scorer_id"] == "ml-shadow-scorer-v1"
    assert payload["scorer_contract"]["frozen_formula_id"] == "hybrid_rank_mean_50_50"
    assert payload["spec_ready_for_implementation"] is True
    assert payload["recommended_next_stage"] == "implement_ml_shadow_scorer_v1_disabled_by_default"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (("metadata", "artifact_type"), "wrong", "artifact_type"),
        (("metadata", "gates_version"), "wrong", "gates_version"),
    ],
)
def test_rejects_wrong_gates_artifact_type_or_version(
    tmp_path: Path, field: tuple[str, str], value: str, message: str
) -> None:
    gates = _gates_payload()
    gates[field[0]][field[1]] = value

    with pytest.raises(MLShadowScorerSpecError, match=message):
        _build(tmp_path, gates=gates)


def test_rejects_if_confirmatory_validation_passed_false(tmp_path: Path) -> None:
    gates = _gates_payload()
    gates["confirmatory_validation_passed"] = False

    with pytest.raises(MLShadowScorerSpecError, match="confirmatory_validation_passed"):
        _build(tmp_path, gates=gates)


def test_rejects_if_fresh_surface_hybrid_validation_passed_false(tmp_path: Path) -> None:
    gates = _gates_payload()
    gates["fresh_surface_hybrid_validation_passed"] = False

    with pytest.raises(MLShadowScorerSpecError, match="fresh_surface_hybrid_validation_passed"):
        _build(tmp_path, gates=gates)


def test_rejects_wrong_recommended_next_stage(tmp_path: Path) -> None:
    gates = _gates_payload()
    gates["recommended_next_stage"] = "implement_ml_shadow_scorer_v1_disabled_by_default"

    with pytest.raises(MLShadowScorerSpecError, match="recommended_next_stage"):
        _build(tmp_path, gates=gates)


def test_rejects_primary_arm_mismatch_across_artifacts(tmp_path: Path) -> None:
    policy = _policy_payload()
    policy["frozen_hybrid_arms"]["primary_confirmatory_arm"] = "hybrid_rank_mean_25_75_heuristic"

    with pytest.raises(MLShadowScorerSpecError, match="primary"):
        _build(tmp_path, policy=policy)


def test_rejects_candidate_pool_work_set_sha_mismatch(tmp_path: Path) -> None:
    surface = _surface_payload()
    surface["candidate_pool"]["candidate_work_set_sha256"] = "different-sha"

    with pytest.raises(MLShadowScorerSpecError, match="candidate_pool_work_set_sha256"):
        _build(tmp_path, surface=surface)


@pytest.mark.parametrize(
    ("target", "value"),
    [
        ("ranking_run_id", "rank-other"),
        ("family", "undercited"),
        ("corpus_snapshot_version", "source-snapshot-other"),
        ("embedding_version", "embedding-other"),
    ],
)
def test_rejects_run_identity_mismatches(tmp_path: Path, target: str, value: str) -> None:
    validation = _validation_payload()
    validation["metadata"][target] = value
    if target in validation["validation_scope"]:
        validation["validation_scope"][target] = value

    with pytest.raises(MLShadowScorerSpecError, match="cross-artifact provenance"):
        _build(tmp_path, validation=validation)


def test_rejects_if_shadow_or_prod_flags_true(tmp_path: Path) -> None:
    gates = _gates_payload()
    gates["shadow_scoring_allowed"] = True

    with pytest.raises(MLShadowScorerSpecError, match="shadow_scoring_allowed"):
        _build(tmp_path, gates=gates)


def test_rejects_if_blockers_include_confirmatory_not_complete(tmp_path: Path) -> None:
    gates = _gates_payload()
    gates["shadow_blockers"].append("confirmatory_validation_not_complete")

    with pytest.raises(MLShadowScorerSpecError, match="confirmatory_validation_not_complete"):
        _build(tmp_path, gates=gates)


def test_formula_and_rank_percentile_policy_are_exact(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["scoring_formula"]["scoring_formula_literal"] == (
        "score = 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)"
    )
    rank_policy = payload["rank_percentile_policy"]
    assert rank_policy["higher_raw_score_is_better"] is True
    assert rank_policy["ties"] == "average rank"
    assert rank_policy["n_equals_1_behavior"] == "rank_pct = 1.0"
    assert "average_rank" in rank_policy["otherwise"]


def test_forbidden_features_include_labels_notes_sample_reason_row_id_and_assignment(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    forbidden = set(payload["forbidden_inputs"])

    for item in (
        "relevance_label",
        "novelty_label",
        "bridge_like_label",
        "good_or_acceptable",
        "label_any_positive",
        "reviewer_notes",
        "sample_reason",
        "row_id",
        "holdout assignment",
    ):
        assert item in forbidden


def test_markdown_states_spec_does_not_authorize_shadow_or_production(tmp_path: Path) -> None:
    md = markdown_from_ml_shadow_scorer_spec(_build(tmp_path))

    assert "does not authorize shadow execution" in md
    assert "does not authorize production default" in md


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "shadow-spec.json"
    out_md = tmp_path / "shadow-spec.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-spec",
        "--hybrid-validation-metric-gates",
        str(paths["hybrid_validation_metric_gates_path"]),
        "--hybrid-validation-on-fresh-surface",
        str(paths["hybrid_validation_on_fresh_surface_path"]),
        "--fresh-eval-surface",
        str(paths["fresh_eval_surface_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--audit-embedding-scorer-export",
        str(paths["audit_embedding_scorer_export_path"]),
        "--hybrid-experiment-spec",
        str(paths["hybrid_experiment_spec_path"]),
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
    assert data["spec_ready_for_implementation"] is True
    assert "ML Shadow Scorer Spec" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_spec.py").read_text(encoding="utf-8").lower()
    test_source = Path(__file__).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    assert "psycopg" not in import_lines
    assert "postgres" not in import_lines
    assert "sklearn" not in import_lines
    assert "openai" not in import_lines
    assert "openalex" not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-spec"')
    end = cli_source.index("ml_fresh_eval_labeling_plan_hybrid_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
