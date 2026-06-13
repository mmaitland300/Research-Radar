"""Tests for second fresh hybrid candidate plan for shadow generalization."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
import pipeline.ml_shadow_scorer_second_hybrid_candidate_plan as plan_mod
from pipeline.ml_shadow_scorer_second_hybrid_candidate_plan import (
    MLShadowScorerSecondHybridCandidatePlanError,
    PLAN_VERSION,
    _work_set_sha256,
    build_ml_shadow_scorer_second_hybrid_candidate_plan_payload,
)
from pipeline.ml_label_dataset import sha256_file


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _candidate(work_id: str, *, bucket_id: str = "music_recommender_systems") -> dict:
    return {
        "openalex_id": f"https://openalex.org/{work_id}",
        "title": f"Candidate {work_id}",
        "year": 2025,
        "citation_count": 3,
        "source_display_name": "Fixture Journal",
        "bucket_id": bucket_id,
        "inclusion_reason": "fixture",
        "matched_terms": ["music"],
    }


def _corpus_plan(
    *,
    new_count: int = 120,
    old_count: int = 3,
    first_count: int = 7,
    include_negative_bucket: bool = True,
) -> dict:
    rows: list[dict] = []
    rows.extend(_candidate(f"W90{i:02d}") for i in range(1, old_count + 1))
    rows.extend(_candidate(f"W80{i:02d}") for i in range(1, first_count + 1))
    rows.extend(_candidate(f"W70{i:02d}") for i in range(1, 3))
    for idx in range(1, new_count + 1):
        bucket = "audio_ml_signal_processing" if include_negative_bucket and idx <= 12 else "music_recommender_systems"
        rows.append(_candidate(f"W{idx}", bucket_id=bucket))
    rows.append(_candidate("W1"))
    return {
        "contact_mode": "cli",
        "contact_provided": True,
        "auth_mode": "no_key",
        "api_key_provided": False,
        "selected_total": len(rows),
        "selected_candidates": rows,
        "dedup_statistics": {"unique_openalex_ids_kept": len(rows) - 1},
    }


def _expansion_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_candidate_source_expansion_plan",
            "plan_version": "ml-shadow-scorer-v1-second-candidate-source-expansion-plan-v1",
        },
        "second_candidate_source_expansion_plan_defined": True,
        "recommended_next_stage": "implement_or_run_second_fresh_candidate_source_build_for_shadow_generalization_v1",
        "current_blocker_summary": {
            "best_distinct_ranking_run_id": "rank-3904fec89d",
            "best_distinct_candidate_pool_work_set_sha256": "underpowered-sha-fixture",
            "best_candidate_work_count": 59,
            "best_confirmatory_eligible_work_count": 43,
            "minimum_confirmatory_eligible_work_count": 100,
            "candidate_gap": 57,
        },
    }


def _audit_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_plan",
            "plan_version": "ml-shadow-scorer-v1-generalization-audit-v1",
        },
        "generalization_audit_plan_defined": True,
        "runtime_implementation_authorized": False,
    }


def _fresh_policy_payload(old_sha: str) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
            "disallowed_eval_work_set_sha256": old_sha,
        },
        "label_policy": {
            "minimum_confirmatory_label_thresholds": {
                "minimum_candidate_work_count": 100,
                "minimum_confirmatory_labeled_work_count": 100,
                "minimum_confirmatory_positive_work_count": 50,
                "minimum_confirmatory_negative_work_count": 20,
                "minimum_distinct_negative_work_count": 20,
                "minimum_confirmatory_label_coverage_rate": 0.60,
            }
        },
    }


def _offline_scoring_payload(old_ids: list[str], old_sha: str) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_scoring",
            "experiment_version": "ml-offline-production-candidate-scoring-v3",
            "eval_work_set_sha256": old_sha,
        },
        "candidate_pool_rows": [{"canonical_openalex_work_id": work_id} for work_id in old_ids],
    }


def _first_surface_payload(first_ids: list[str], first_sha: str) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
        },
        "candidate_pool": {
            "candidate_work_set_sha256": first_sha,
            "candidate_rows": [{"canonical_openalex_work_id": work_id} for work_id in first_ids],
        },
    }


def _second_surface_payload(*, full_underpowered: bool = False, underpowered_ids: list[str] | None = None) -> dict:
    underpowered_ids = underpowered_ids or [f"W70{i:02d}" for i in range(1, 6)]
    source = {
        "ranking_run_id": "rank-3904fec89d",
        "candidate_pool_work_set_sha256": _work_set_sha256(underpowered_ids),
        "candidate_pool_work_count": len(underpowered_ids),
        "confirmatory_metric_eligible_work_count": 43,
        "distinct_from_first_validated_surface": True,
        "disallowed_reasons": [],
        "candidate_row_preview": [{"canonical_openalex_work_id": underpowered_ids[0]}],
        "overlap_work_ids_preview": [underpowered_ids[1]],
    }
    if full_underpowered:
        source["candidate_pool_work_ids"] = list(underpowered_ids)
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_second_surface",
            "surface_version": "ml-shadow-scorer-v1-generalization-second-surface-v1",
        },
        "candidate_sources_considered": [source],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    expansion_plan: dict | None = None,
    offline_scoring: dict | None = None,
    first_surface: dict | None = None,
    second_surface: dict | None = None,
) -> dict[str, Path]:
    old_ids = [f"W90{i:02d}" for i in range(1, 6)]
    first_ids = [f"W80{i:02d}" for i in range(1, 11)]
    old_sha = _work_set_sha256(old_ids)
    first_sha = _work_set_sha256(first_ids)
    underpowered_ids = [f"W70{i:02d}" for i in range(1, 6)]
    underpowered_sha = _work_set_sha256(underpowered_ids)
    monkeypatch.setattr(plan_mod, "OLD_217_EVAL_SHA", old_sha)
    monkeypatch.setattr(plan_mod, "FIRST_VALIDATED_SURFACE_SHA", first_sha)
    monkeypatch.setattr(plan_mod, "UNDERPOWERED_SOURCE_SHA", underpowered_sha)
    conflict_path = tmp_path / "conflict.md"
    conflict_path.write_text("# Conflict Policy\n", encoding="utf-8")
    return {
        "second_candidate_source_expansion_plan_path": _write_json(
            tmp_path, "expansion-plan.json", expansion_plan or _expansion_plan_payload()
        ),
        "generalization_audit_plan_path": _write_json(tmp_path, "audit-plan.json", _audit_plan_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", _fresh_policy_payload(old_sha)),
        "label_dataset_path": _write_json(
            tmp_path, "labels.json", {"dataset_version": "ml-label-dataset-v10", "rows": [{"row_id": "not-used"}]}
        ),
        "conflict_policy_path": conflict_path,
        "offline_production_candidate_scoring_v3_path": _write_json(
            tmp_path, "scoring-v3.json", offline_scoring or _offline_scoring_payload(old_ids, old_sha)
        ),
        "first_validated_surface_path": _write_json(
            tmp_path, "first-surface.json", first_surface or _first_surface_payload(first_ids, first_sha)
        ),
        "generalization_second_surface_path": _write_json(
            tmp_path, "second-surface.json", second_surface or _second_surface_payload(underpowered_ids=underpowered_ids)
        ),
    }


def _build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    corpus_plan: dict | None = None,
    **kwargs: object,
) -> dict:
    return build_ml_shadow_scorer_second_hybrid_candidate_plan_payload(
        **_paths(tmp_path, monkeypatch, **kwargs),
        corpus_candidate_plan=corpus_plan or _corpus_plan(),
        target_min=180,
        target_max=600,
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
    )


def test_happy_path_mocked_planner_meets_threshold_after_exclusions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_hybrid_candidate_plan"
    assert payload["metadata"]["plan_version"] == PLAN_VERSION
    assert payload["second_hybrid_candidate_plan_defined"] is True
    assert payload["readiness_estimate"]["estimated_overlap_with_old_217"] == 3
    assert payload["readiness_estimate"]["estimated_overlap_with_first_validated_surface"] == 7
    assert payload["readiness_estimate"]["estimated_confirmatory_eligible_after_exclusions"] >= 100
    assert payload["readiness_estimate"]["candidate_threshold_plausibly_met"] is True
    assert payload["recommended_next_stage"] == "ingest_second_hybrid_candidate_plan_as_snapshot_v1"


def test_dedupes_canonical_openalex_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    ids = [row["canonical_openalex_work_id"] for row in payload["candidate_selection"]["selected_candidates"]]

    assert len(ids) == len(set(ids))
    assert ids.count("W1") == 1


def test_tags_first_validated_and_old_217_overlaps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    rows = {row["canonical_openalex_work_id"]: row for row in payload["candidate_selection"]["selected_candidates"]}

    assert rows["W9001"]["old_217_overlap"] is True
    assert rows["W8001"]["first_validated_surface_overlap"] is True
    assert rows["W1"]["confirmatory_metric_candidate_after_exclusions"] is True


def test_rejects_v3_sha_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    old_ids = [f"W90{i:02d}" for i in range(1, 6)]
    bad_scoring = _offline_scoring_payload(old_ids, "bad")
    with pytest.raises(MLShadowScorerSecondHybridCandidatePlanError, match="eval_work_set_sha256"):
        _build(tmp_path, monkeypatch, offline_scoring=bad_scoring)


def test_preview_only_underpowered_overlap_is_not_claimed_as_full_pool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["readiness_estimate"]["full_underpowered_overlap_available"] is False
    assert payload["readiness_estimate"]["underpowered_source_overlap_preview_count"] == 2
    assert "estimated_overlap_with_underpowered_source" not in payload["readiness_estimate"]
    assert payload["readiness_estimate"]["estimated_overlap_with_underpowered_source_preview"] == 2
    assert payload["readiness_estimate"]["underpowered_candidate_pool_work_count"] == 5


def test_full_underpowered_overlap_exact_when_full_id_list_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _build(
        tmp_path,
        monkeypatch,
        second_surface=_second_surface_payload(full_underpowered=True),
    )

    assert payload["readiness_estimate"]["full_underpowered_overlap_available"] is True
    assert payload["readiness_estimate"]["estimated_overlap_with_underpowered_source"] == 2
    assert payload["readiness_estimate"]["underpowered_source_overlap_preview_count"] == 0


def test_includes_borderline_negative_rollup_bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    rollups = payload["bucket_summary"]["rollups"]
    assert rollups["borderline_or_negative_candidate"]["selected_count"] > 0
    assert rollups["MIR/audio_candidate"]["selected_count"] > 0


def test_does_not_use_labels_for_selection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["candidate_selection"]["label_dataset_used_for_selection"] is False
    assert all(row["label_used_for_selection"] is False for row in payload["candidate_selection"]["selected_candidates"])


def test_candidate_threshold_false_when_estimated_eligible_below_100(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _build(tmp_path, monkeypatch, corpus_plan=_corpus_plan(new_count=80, old_count=3, first_count=7))

    assert payload["readiness_estimate"]["estimated_confirmatory_eligible_after_exclusions"] < 100
    assert payload["readiness_estimate"]["candidate_threshold_plausibly_met"] is False
    assert payload["recommended_next_stage"] == "revise_second_candidate_plan_queries"


def test_cli_writes_json_and_markdown_with_mocked_planner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path, monkeypatch)
    out_json = tmp_path / "second-hybrid-plan.json"
    out_md = tmp_path / "second-hybrid-plan.md"
    monkeypatch.setattr(plan_mod, "run_corpus_v2_candidate_plan", lambda **kwargs: _corpus_plan())

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-second-hybrid-candidate-plan",
        "--second-candidate-source-expansion-plan",
        str(paths["second_candidate_source_expansion_plan_path"]),
        "--generalization-audit-plan",
        str(paths["generalization_audit_plan_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--offline-production-candidate-scoring-v3",
        str(paths["offline_production_candidate_scoring_v3_path"]),
        "--first-validated-surface",
        str(paths["first_validated_surface_path"]),
        "--generalization-second-surface",
        str(paths["generalization_second_surface_path"]),
        "--mailto",
        "reviewer@example.test",
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["readiness_estimate"]["candidate_threshold_plausibly_met"] is True
    assert "Bucket Composition" in out_md.read_text(encoding="utf-8")


def test_no_db_imports_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_hybrid_candidate_plan.py").read_text(
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

    cli_source = read_cli_parser_source(PACKAGE_ROOT)
    start = cli_source.index('"ml-shadow-scorer-second-hybrid-candidate-plan"')
    end = cli_source.index("ml_fresh_product_candidate_ranking_source_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
