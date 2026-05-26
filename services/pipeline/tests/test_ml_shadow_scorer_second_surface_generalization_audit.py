"""Tests for second-surface ml-shadow-scorer-v1 generalization audit."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.ml_shadow_scorer_second_surface_generalization_audit as audit_module
from pipeline.ml_shadow_scorer_second_surface_generalization_audit import (
    FORMULA_ID,
    LABEL_REVIEW_POOL_VARIANT,
    MLShadowScorerSecondSurfaceGeneralizationAuditError,
    build_ml_shadow_scorer_second_surface_generalization_audit_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]


def _candidate_rows() -> list[dict]:
    return [
        {
            "canonical_openalex_work_id": "W000000001",
            "title": "One",
            "year": 2024,
            "ranking_run_id": audit_module.RANKING_RUN_ID,
            "family": audit_module.FAMILY,
            "corpus_snapshot_version": audit_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": audit_module.EMBEDDING_VERSION,
            "final_score": 0.9,
            "audit_embedding_probability_work": 0.1,
            "scorer_version": "ml-offline-audit-embedding-scorer-v2",
        },
        {
            "canonical_openalex_work_id": "W000000002",
            "title": "Two",
            "year": 2024,
            "ranking_run_id": audit_module.RANKING_RUN_ID,
            "family": audit_module.FAMILY,
            "corpus_snapshot_version": audit_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": audit_module.EMBEDDING_VERSION,
            "final_score": 0.8,
            "audit_embedding_probability_work": 0.9,
            "scorer_version": "ml-offline-audit-embedding-scorer-v2",
        },
        {
            "canonical_openalex_work_id": "W000000003",
            "title": "Three",
            "year": 2024,
            "ranking_run_id": audit_module.RANKING_RUN_ID,
            "family": audit_module.FAMILY,
            "corpus_snapshot_version": audit_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": audit_module.EMBEDDING_VERSION,
            "final_score": 0.1,
            "audit_embedding_probability_work": 0.8,
            "scorer_version": "ml-offline-audit-embedding-scorer-v2",
        },
        {
            "canonical_openalex_work_id": "W000000004",
            "title": "Prior overlap only",
            "year": 2024,
            "ranking_run_id": audit_module.RANKING_RUN_ID,
            "family": audit_module.FAMILY,
            "corpus_snapshot_version": audit_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": audit_module.EMBEDDING_VERSION,
            "final_score": 0.2,
            "audit_embedding_probability_work": 0.2,
            "scorer_version": "ml-offline-audit-embedding-scorer-v2",
        },
    ]


def _patch_small_constants(monkeypatch: pytest.MonkeyPatch) -> str:
    candidate_sha = audit_module._work_set_sha256([row["canonical_openalex_work_id"] for row in _candidate_rows()])
    monkeypatch.setattr(audit_module, "EXPECTED_CANDIDATE_POOL_WORK_COUNT", 4)
    monkeypatch.setattr(audit_module, "EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT", 3)
    monkeypatch.setattr(audit_module, "EXPECTED_POSITIVE_COUNT", 2)
    monkeypatch.setattr(audit_module, "EXPECTED_NEGATIVE_COUNT", 1)
    monkeypatch.setattr(audit_module, "EXPECTED_CANDIDATE_SHA", candidate_sha)
    monkeypatch.setattr(audit_module, "EXPECTED_OLD_217_OVERLAP_COUNT", 1)
    monkeypatch.setattr(audit_module, "EXPECTED_FIRST_SURFACE_OVERLAP_COUNT", 1)
    monkeypatch.setattr(audit_module, "EXPECTED_COMBINED_PRIOR_OVERLAP_COUNT", 1)
    return candidate_sha


def _discovery_payload(candidate_sha: str, *, status: str = "selected_ready_for_generalization_audit") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_second_surface",
            "surface_version": "ml-shadow-scorer-v1-generalization-second-surface-v1",
            "source_label_dataset_version": "ml-label-dataset-v11",
        },
        "discovery_summary": {
            "status": status,
            "recommended_next_stage": "audit_ml_shadow_scorer_v1_on_second_fresh_surface",
        },
        "selected_second_surface": {
            "ranking_run_id": audit_module.RANKING_RUN_ID,
            "family": audit_module.FAMILY,
            "corpus_snapshot_version": audit_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": audit_module.EMBEDDING_VERSION,
            "candidate_pool_work_count": 4,
            "confirmatory_metric_eligible_work_count": 3,
            "candidate_pool_work_set_sha256": candidate_sha,
        },
        "threshold_check": {
            "final_score_coverage": {"observed": 4, "threshold": 4, "passed": True},
            "learned_probability_coverage": {"observed": 4, "threshold": 4, "passed": True},
            "minimum_confirmatory_labeled_work_count": {"observed": 3, "threshold": 3, "passed": True},
            "minimum_confirmatory_positive_work_count": {"observed": 2, "threshold": 2, "passed": True},
            "minimum_confirmatory_negative_work_count": {"observed": 1, "threshold": 1, "passed": True},
            "minimum_distinct_negative_work_count": {"observed": 1, "threshold": 1, "passed": True},
            "minimum_confirmatory_label_coverage_rate": {"observed": 1.0, "threshold": 0.6, "passed": True},
            "unresolved_label_conflicts": {"observed": 0, "threshold": 0, "passed": True},
        },
        "overlap_report": {
            "old_217_overlap_count": 1,
            "rank_9f4b2a2084_overlap_count": 1,
            "combined_prior_surface_overlap_count": 1,
            "confirmatory_denominator_excludes_prior_overlaps": True,
        },
        "label_coverage": {
            "work_level": {
                "confirmatory_labeled_work_count": 3,
                "confirmatory_positive_work_count": 2,
                "confirmatory_negative_work_count": 1,
                "label_coverage_rate": 1.0,
                "conflicting_target_work_group_count": 0,
            }
        },
        "readiness_for_generalization_audit": {"ready_for_generalization_audit_execution": True},
        "recommended_next_stage": "audit_ml_shadow_scorer_v1_on_second_fresh_surface",
    }


def _learned_probability_payload(candidate_sha: str, *, coverage_count: int = 4, rows: list[dict] | None = None) -> dict:
    candidate_rows = [dict(row, candidate_pool_work_set_sha256=candidate_sha) for row in (rows or _candidate_rows())]
    missing = 4 - coverage_count
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_surface_learned_probability",
            "artifact_version": "ml-shadow-scorer-v1-second-surface-learned-probability-v1",
            "ranking_run_id": audit_module.RANKING_RUN_ID,
            "family": audit_module.FAMILY,
            "corpus_snapshot_version": audit_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": audit_module.EMBEDDING_VERSION,
            "candidate_pool_work_set_sha256": candidate_sha,
        },
        "execution_summary": {
            "status": "succeeded",
            "candidate_pool_work_count": 4,
            "output_row_count": coverage_count,
            "learned_probability_coverage_count": coverage_count,
            "missing_learned_probability_count": missing,
        },
        "coverage_summary": {
            "learned_probability_coverage_count": coverage_count,
            "missing_probability_count": missing,
        },
        "candidate_work_scores": candidate_rows[:coverage_count],
    }


def _label_rows(*, swapped: bool = False, duplicate_conflict: bool = False) -> list[dict]:
    labels = {
        "W000000001": (False, "miss") if swapped else (True, "good"),
        "W000000002": (True, "good"),
        "W000000003": (True, "acceptable") if swapped else (False, "miss"),
    }
    rows = []
    for index, (work_id, (target, relevance)) in enumerate(labels.items(), start=1):
        rows.append(
            {
                "dataset_version": "ml-label-dataset-v11",
                "row_id": f"row-{index}",
                "work_id": work_id,
                "ranking_run_id": audit_module.RANKING_RUN_ID,
                "family": audit_module.FAMILY,
                "review_pool_variant": LABEL_REVIEW_POOL_VARIANT,
                "relevance_label": relevance,
                "novelty_label": "useful",
                "bridge_like_label": "not_applicable",
                "good_or_acceptable": target,
            }
        )
    if duplicate_conflict:
        rows[2] = {
            **rows[2],
            "row_id": "row-duplicate",
            "work_id": "W000000001",
            "good_or_acceptable": False,
            "relevance_label": "miss",
        }
    return rows


def _label_dataset_payload(candidate_sha: str, *, rows: list[dict] | None = None) -> dict:
    return {
        "dataset_version": "ml-label-dataset-v11",
        "metadata": {
            "dataset_version": "ml-label-dataset-v11",
            "shadow_generalization_second_surface_v1_ingest": {
                "ranking_run_id": audit_module.RANKING_RUN_ID,
                "family": audit_module.FAMILY,
                "candidate_pool_work_set_sha256": candidate_sha,
                "confirmatory_metric_eligible_work_count": 3,
                "labeled_count": 3,
                "positive_count": 2,
                "negative_count": 1,
                "label_thresholds_passed": True,
            },
        },
        "rows": rows or _label_rows(),
    }


def _spec_payload() -> dict:
    return {
        "metadata": {"artifact_type": "ml_shadow_scorer_spec", "spec_version": "ml-shadow-scorer-v1-spec"},
        "scoring_formula": {"formula_id": FORMULA_ID},
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }


def _generalization_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_plan",
            "plan_version": "ml-shadow-scorer-v1-generalization-audit-v1",
        },
        "generalization_audit_plan_defined": True,
        "runtime_implementation_authorized": False,
    }


def _fresh_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        },
        "gate_linkage": {"material_lift_thresholds": {"delta_roc_auc_gte": 0.03, "or_delta_average_precision_gte": 0.02}},
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
    monkeypatch: pytest.MonkeyPatch,
    *,
    discovery: dict | None = None,
    learned: dict | None = None,
    labels: dict | None = None,
    spec: dict | None = None,
    audit_plan: dict | None = None,
    fresh_policy: dict | None = None,
    online_policy: dict | None = None,
) -> dict[str, Path]:
    candidate_sha = _patch_small_constants(monkeypatch)
    return {
        "generalization_second_surface_path": _write_json(
            tmp_path, "discovery.json", discovery or _discovery_payload(candidate_sha)
        ),
        "learned_probability_artifact_path": _write_json(
            tmp_path, "learned.json", learned or _learned_probability_payload(candidate_sha)
        ),
        "label_dataset_path": _write_json(tmp_path, "labels.json", labels or _label_dataset_payload(candidate_sha)),
        "shadow_scorer_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "generalization_audit_plan_path": _write_json(
            tmp_path, "generalization-plan.json", audit_plan or _generalization_plan_payload()
        ),
        "fresh_surface_policy_path": _write_json(tmp_path, "fresh-policy.json", fresh_policy or _fresh_policy_payload()),
        "online_shadow_policy_path": _write_json(tmp_path, "online-policy.json", online_policy or _online_policy_payload()),
    }


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **overrides: dict) -> dict:
    return build_ml_shadow_scorer_second_surface_generalization_audit_payload(
        **_paths(tmp_path, monkeypatch, **overrides),
        repo_root=tmp_path,
        generated_at="2026-05-25T00:00:00Z",
    )


def test_happy_path_small_fixture_computes_scores_labels_and_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["generalization_audit_executed"] is True
    assert payload["generalization_audit_gates_passed"] is False
    assert payload["audit_scope"]["candidate_pool_work_count"] == 4
    assert payload["metric_coverage"]["confirmatory_metric_work_count"] == 3
    assert payload["label_join_summary"]["metric_positive_count"] == 2
    assert payload["label_join_summary"]["metric_negative_count"] == 1
    assert set(payload["arm_metrics"]) == {"heuristic_final_score_baseline", FORMULA_ID}
    assert payload["arm_metrics"][FORMULA_ID]["roc_auc_mann_whitney"] is not None
    assert payload["comparisons_vs_heuristic"][FORMULA_ID]["generalization_gates_passed"] is False
    assert len(payload["shadow_output_rows"]) == 4
    pool_only = [row for row in payload["shadow_output_rows"] if row["canonical_openalex_work_id"] == "W000000004"][0]
    assert pool_only["confirmatory_metric_eligible"] is False
    assert pool_only["label_any_positive_not_used_for_scoring"] is True


def test_rejects_discovery_not_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    paths = _paths(tmp_path, monkeypatch, discovery=_discovery_payload(sha, status="selected_needs_labels"))
    with pytest.raises(MLShadowScorerSecondSurfaceGeneralizationAuditError, match="discovery_summary.status"):
        build_ml_shadow_scorer_second_surface_generalization_audit_payload(**paths, repo_root=tmp_path)


def test_rejects_failed_discovery_threshold_check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    discovery = _discovery_payload(sha)
    discovery["threshold_check"]["learned_probability_coverage"]["passed"] = False
    paths = _paths(tmp_path, monkeypatch, discovery=discovery)
    with pytest.raises(MLShadowScorerSecondSurfaceGeneralizationAuditError, match="threshold checks failed"):
        build_ml_shadow_scorer_second_surface_generalization_audit_payload(**paths, repo_root=tmp_path)


def test_rejects_learned_probability_coverage_incomplete(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    paths = _paths(tmp_path, monkeypatch, learned=_learned_probability_payload(sha, coverage_count=3))
    with pytest.raises(MLShadowScorerSecondSurfaceGeneralizationAuditError, match="output_row_count"):
        build_ml_shadow_scorer_second_surface_generalization_audit_payload(**paths, repo_root=tmp_path)


def test_rejects_candidate_sha_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    learned = _learned_probability_payload(sha)
    learned["candidate_work_scores"][0]["canonical_openalex_work_id"] = "W999999999"
    paths = _paths(tmp_path, monkeypatch, learned=learned)
    with pytest.raises(MLShadowScorerSecondSurfaceGeneralizationAuditError, match="recomputed candidate_work_scores SHA"):
        build_ml_shadow_scorer_second_surface_generalization_audit_payload(**paths, repo_root=tmp_path)


def test_rejects_missing_or_conflicting_confirmatory_labels(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    missing_label_payload = _label_dataset_payload(sha, rows=_label_rows()[:2])
    with pytest.raises(MLShadowScorerSecondSurfaceGeneralizationAuditError, match="expected 3"):
        build_ml_shadow_scorer_second_surface_generalization_audit_payload(
            **_paths(tmp_path, monkeypatch, labels=missing_label_payload), repo_root=tmp_path
        )

    conflicting_label_payload = _label_dataset_payload(sha, rows=_label_rows(duplicate_conflict=True))
    with pytest.raises(MLShadowScorerSecondSurfaceGeneralizationAuditError, match="conflicting or duplicate"):
        build_ml_shadow_scorer_second_surface_generalization_audit_payload(
            **_paths(tmp_path, monkeypatch, labels=conflicting_label_payload), repo_root=tmp_path
        )


def test_pool_only_prior_overlap_rows_are_scored_but_excluded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    assert payload["metric_coverage"]["pool_only_non_metric_row_count"] == 1
    all_ids = {row["canonical_openalex_work_id"] for row in payload["shadow_output_rows"]}
    metric_ids = {
        row["canonical_openalex_work_id"]
        for row in payload["shadow_output_rows"]
        if row["confirmatory_metric_eligible"] is True
    }
    assert "W000000004" in all_ids
    assert "W000000004" not in metric_ids
    assert payload["leakage_report"]["old_217_overlap_excluded_from_confirmatory_metrics"] is True


def test_labels_do_not_change_shadow_scores_or_ranks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    base = build_ml_shadow_scorer_second_surface_generalization_audit_payload(
        **_paths(tmp_path / "base", monkeypatch, labels=_label_dataset_payload(sha, rows=_label_rows())),
        repo_root=tmp_path,
    )
    swapped = build_ml_shadow_scorer_second_surface_generalization_audit_payload(
        **_paths(tmp_path / "swapped", monkeypatch, labels=_label_dataset_payload(sha, rows=_label_rows(swapped=True))),
        repo_root=tmp_path,
    )
    base_scores = {
        row["canonical_openalex_work_id"]: (row["shadow_rank"], row["ml_shadow_scorer_v1_score"])
        for row in base["shadow_output_rows"]
    }
    swapped_scores = {
        row["canonical_openalex_work_id"]: (row["shadow_rank"], row["ml_shadow_scorer_v1_score"])
        for row in swapped["shadow_output_rows"]
    }
    assert base_scores == swapped_scores


def test_material_lift_thresholds_are_copied_from_policy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    thresholds = payload["confirmatory_decision_inputs"]["material_lift_thresholds"]
    assert thresholds == {"delta_roc_auc_gte": 0.03, "or_delta_average_precision_gte": 0.02}


def test_blockers_and_shadow_prod_runtime_flags_remain_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["missing_generalization_audit_on_second_surface"] is False
    assert blockers["missing_generalization_audit_gates"] is True
    assert blockers["runtime_implementation_authorized"] is False
    assert blockers["online_shadow_execution_enabled"] is False
    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "audit.json"
    out_md = tmp_path / "audit.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-second-surface-generalization-audit",
        "--generalization-second-surface",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-generalization-second-surface-v1.json"),
        "--learned-probability-artifact",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json"),
        "--label-dataset",
        str(REPO_ROOT / "docs/audit/ml-label-dataset-v11.json"),
        "--shadow-scorer-spec",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-spec.json"),
        "--generalization-audit-plan",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-generalization-audit-v1.json"),
        "--fresh-surface-policy",
        str(REPO_ROOT / "docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json"),
        "--online-shadow-policy",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json"),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, check=True, text=True, capture_output=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_surface_generalization_audit"
    assert payload["audit_scope"]["confirmatory_metric_work_count"] == 168
    assert "Second-Surface ml-shadow-scorer-v1 Generalization Audit" in out_md.read_text(encoding="utf-8")
    assert "run_ml_shadow_scorer_v1_generalization_audit_gates_v1" in result.stdout


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_surface_generalization_audit.py"
    ).read_text(encoding="utf-8")
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-second-surface-generalization-audit"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
