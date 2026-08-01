"""Tests for second-surface discovery for ml-shadow-scorer-v1 generalization."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.ml_shadow_scorer_generalization_second_surface as second_surface
from pipeline.ml_shadow_scorer_generalization_second_surface import (
    DISALLOWED_CANDIDATE_SHA,
    DISALLOWED_RANKING_RUN_ID,
    MLShadowScorerGeneralizationSecondSurfaceError,
    OLD_217_EVAL_SHA,
    SURFACE_VERSION,
    assert_local_database_url,
    build_ml_shadow_scorer_generalization_second_surface_payload,
    write_ml_shadow_scorer_generalization_second_surface,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_plan",
            "plan_version": "ml-shadow-scorer-v1-generalization-audit-v1",
        },
        "generalization_audit_plan_defined": True,
        "generalization_audit_executed": False,
        "runtime_implementation_authorized": False,
        "recommended_next_stage": "materialize_or_select_second_fresh_surface_for_shadow_generalization_v1",
        "shadow_and_production_blockers": {"missing_generalization_audit_plan_v1": False},
    }


def _online_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_policy",
            "policy_version": "ml-shadow-scorer-v1-online-shadow-policy",
        },
        "runtime_implementation_authorized": False,
    }


def _fresh_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
            "disallowed_eval_work_set_sha256": OLD_217_EVAL_SHA,
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


def _label_dataset_payload(ids: list[str] | None = None, *, positives: int = 60, version: str = "ml-label-dataset-v11") -> dict:
    ids = ids or [f"W{i:06d}" for i in range(1, 121)]
    rows = []
    for index, work_id in enumerate(ids):
        rows.append(
            {
                "row_id": f"label-{work_id}",
                "work_id": work_id,
                "openalex_work_id": work_id,
                "relevance_label": "good" if index < positives else "miss",
                "novelty_label": "useful",
                "bridge_like_label": "partial",
                "good_or_acceptable": index < positives,
            }
        )
    return {"dataset_version": version, "metadata": {"dataset_version": version}, "rows": rows}


def _offline_scoring_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_scoring",
            "experiment_version": "ml-offline-production-candidate-scoring-v3",
            "eval_work_set_sha256": OLD_217_EVAL_SHA,
        },
        "candidate_pool_rows": [{"canonical_openalex_work_id": "WOLD000"}],
    }


def _first_surface_payload() -> dict:
    return {
        "metadata": {"artifact_type": "ml_fresh_eval_surface_hybrid"},
        "candidate_pool": {
            "candidate_work_set_sha256": DISALLOWED_CANDIDATE_SHA,
            "candidate_rows": [
                {"canonical_openalex_work_id": "W000121"},
                {"canonical_openalex_work_id": "W000122"},
            ],
        },
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    plan: dict | None = None,
    policy: dict | None = None,
    fresh_policy: dict | None = None,
    labels: dict | None = None,
    scoring: dict | None = None,
    first_surface: dict | None = None,
) -> dict[str, Path]:
    conflict = tmp_path / "conflict.md"
    conflict.write_text("# Conflict policy\n", encoding="utf-8")
    return {
        "generalization_audit_plan_path": _write_json(tmp_path, "plan.json", plan or _plan_payload()),
        "online_shadow_policy_path": _write_json(tmp_path, "online-policy.json", policy or _online_policy_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "fresh-policy.json", fresh_policy or _fresh_policy_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", labels or _label_dataset_payload()),
        "conflict_policy_path": conflict,
        "offline_production_candidate_scoring_v3_path": _write_json(
            tmp_path, "scoring-v3.json", scoring or _offline_scoring_payload()
        ),
        "first_validated_surface_path": _write_json(
            tmp_path, "first-surface.json", first_surface or _first_surface_payload()
        ),
    }


def _candidate_rows(prefix: str, count: int, *, missing_final_score: bool = False) -> list[dict]:
    rows = []
    base = sum((idx + 1) * ord(char) for idx, char in enumerate(prefix)) * 10_000
    for index in range(1, count + 1):
        work_id = f"W{base + index:09d}"
        rows.append(
            {
                "ranking_run_id": "rank-second",
                "family": "emerging",
                "heuristic_rank": index,
                "internal_work_id": index,
                "canonical_openalex_work_id": work_id,
                "openalex_id": work_id,
                "title": f"Work {index}",
                "year": 2020,
                "citation_count": index,
                "corpus_snapshot_version": "source-snapshot-second",
                "final_score": None if missing_final_score and index == 1 else float(count - index),
            }
        )
    return rows


def _install_query_fixtures(
    monkeypatch: pytest.MonkeyPatch,
    sources: list[dict],
    pools: dict[str, list[dict]],
    *,
    old_ids: set[str] | None = None,
    probability_full_for: set[str] | None = None,
) -> None:
    monkeypatch.setattr(second_surface, "_old_eval_ids_from_v3", lambda payload: old_ids or {"WOLD000"})
    monkeypatch.setattr(
        second_surface,
        "_query_candidate_source_rows",
        lambda conn, family, ranking_run_id=None: [
            source for source in sources if ranking_run_id is None or source["ranking_run_id"] == ranking_run_id
        ],
    )
    monkeypatch.setattr(
        second_surface,
        "_query_candidate_pool",
        lambda conn, ranking_run_id, family: pools.get(ranking_run_id, []),
    )
    monkeypatch.setattr(
        second_surface,
        "_query_embedding_coverage_count",
        lambda conn, internal_work_ids, embedding_version: len(list(internal_work_ids)),
    )

    def _probe(*, repo_root: Path, ranking_run_id: str, candidate_sha: str | None, candidate_work_count: int) -> dict:
        if ranking_run_id in (probability_full_for or set()):
            return {
                "probe_status": "found",
                "source_artifact_path": "docs/audit/fake.json",
                "learned_probability_coverage_count": candidate_work_count,
                "full_coverage": True,
            }
        return {
            "probe_status": "not_found",
            "source_artifact_path": None,
            "learned_probability_coverage_count": 0,
            "full_coverage": False,
        }

    monkeypatch.setattr(second_surface, "_approved_probability_probe", _probe)


def _source(rid: str, *, finished_at: str = "2026-05-20T00:00:00Z") -> dict:
    return {
        "ranking_run_id": rid,
        "status": "succeeded",
        "ranking_version": "ranking-v",
        "corpus_snapshot_version": "source-snapshot-second",
        "embedding_version": "emb-second",
        "started_at": finished_at,
        "finished_at": finished_at,
    }


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, labels: dict | None = None, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_generalization_second_surface_payload(
        object(),
        **_paths(tmp_path, labels=labels),
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        repo_root=tmp_path,
        generated_at="2026-05-21T00:00:00Z",
        **kwargs,
    )


def test_happy_path_selects_distinct_second_surface(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _candidate_rows("A", 120)
    ids = [row["canonical_openalex_work_id"] for row in pool]
    _install_query_fixtures(
        monkeypatch,
        [_source("rank-second")],
        {"rank-second": pool},
        probability_full_for={"rank-second"},
    )

    payload = _build(tmp_path, monkeypatch, labels=_label_dataset_payload(ids))

    assert payload["discovery_summary"]["status"] == "selected_ready_for_generalization_audit"
    assert payload["selected_second_surface"]["ranking_run_id"] == "rank-second"
    assert payload["readiness_for_generalization_audit"]["ready_for_generalization_audit_execution"] is True
    assert payload["recommended_next_stage"] == "audit_ml_shadow_scorer_v1_on_second_fresh_surface"
    assert payload["shadow_and_production_blockers"]["online_shadow_execution_enabled"] is False
    assert payload["shadow_and_production_blockers"]["production_default_allowed"] is False


def test_rejects_disallowed_rank_and_candidate_sha(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    disallowed_pool = _candidate_rows("D", 120)
    disallowed_ids = {row["canonical_openalex_work_id"] for row in disallowed_pool}
    original_work_set_sha = second_surface._work_set_sha256

    def _fake_sha(ids: list[str]) -> str:
        if set(ids) == disallowed_ids:
            return DISALLOWED_CANDIDATE_SHA
        return original_work_set_sha(ids)

    monkeypatch.setattr(second_surface, "_work_set_sha256", _fake_sha)
    _install_query_fixtures(
        monkeypatch,
        [_source(DISALLOWED_RANKING_RUN_ID), _source("rank-disallowed-sha")],
        {DISALLOWED_RANKING_RUN_ID: _candidate_rows("F", 120), "rank-disallowed-sha": disallowed_pool},
        probability_full_for={DISALLOWED_RANKING_RUN_ID, "rank-disallowed-sha"},
    )

    payload = _build(tmp_path, monkeypatch, labels=_label_dataset_payload([row["canonical_openalex_work_id"] for row in disallowed_pool]))

    assert payload["discovery_summary"]["status"] == "blocked_no_distinct_second_surface"
    reasons = {reason for source in payload["candidate_sources_considered"] for reason in source["disallowed_reasons"]}
    assert "ranking_run_id_matches_first_validated_surface" in reasons
    assert "candidate_sha_matches_first_validated_surface" in reasons


def test_rejects_v3_eval_work_set_sha_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scoring = _offline_scoring_payload()
    scoring["metadata"]["eval_work_set_sha256"] = "bad"
    _install_query_fixtures(monkeypatch, [], {})

    with pytest.raises(MLShadowScorerGeneralizationSecondSurfaceError, match="eval_work_set_sha256"):
        build_ml_shadow_scorer_generalization_second_surface_payload(
            object(),
            **_paths(tmp_path, scoring=scoring),
            repo_root=tmp_path,
        )


def test_computes_old_217_and_first_surface_overlaps_separately(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _candidate_rows("B", 120)
    pool[0]["canonical_openalex_work_id"] = "WOLD001"
    pool[1]["canonical_openalex_work_id"] = "W000121"
    ids = [row["canonical_openalex_work_id"] for row in pool if row["canonical_openalex_work_id"] not in {"WOLD001", "W000121"}]
    _install_query_fixtures(
        monkeypatch,
        [_source("rank-overlap")],
        {"rank-overlap": pool},
        old_ids={"WOLD001"},
        probability_full_for={"rank-overlap"},
    )

    payload = _build(tmp_path, monkeypatch, labels=_label_dataset_payload(ids))
    selected = payload["candidate_sources_considered"][0]

    assert selected["old_217_overlap_count"] == 1
    assert selected["rank_9f4b2a2084_overlap_count"] == 1
    assert selected["combined_prior_surface_overlap_count"] == 2
    assert selected["confirmatory_metric_eligible_work_count"] == 118


def test_selected_needs_labels_when_thresholds_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _candidate_rows("C", 120)
    _install_query_fixtures(
        monkeypatch,
        [_source("rank-needs-labels")],
        {"rank-needs-labels": pool},
        probability_full_for={"rank-needs-labels"},
    )

    payload = _build(tmp_path, monkeypatch, labels=_label_dataset_payload([], positives=0))

    assert payload["discovery_summary"]["status"] == "selected_needs_labels"
    assert payload["recommended_next_stage"] == "create_second_surface_labeling_plan_for_shadow_generalization_v1"


def test_selected_needs_learned_probability_coverage_when_probe_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = _candidate_rows("E", 120)
    ids = [row["canonical_openalex_work_id"] for row in pool]
    _install_query_fixtures(monkeypatch, [_source("rank-needs-prob")], {"rank-needs-prob": pool})

    payload = _build(tmp_path, monkeypatch, labels=_label_dataset_payload(ids))

    assert payload["discovery_summary"]["status"] == "selected_needs_learned_probability_coverage"
    assert payload["learned_probability_coverage"]["learned_probability_coverage_count"] == 0
    assert payload["recommended_next_stage"] == "create_second_surface_learned_probability_coverage_plan_v1"


def test_real_probability_probe_reads_second_surface_artifact_and_preserves_legacy_probe(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "docs/audit"
    artifact_dir.mkdir(parents=True)
    ranking_run_id = "rank-83787b91ef"
    candidate_sha = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
    second_surface_artifact = artifact_dir / "ml-shadow-scorer-v1-second-surface-learned-probability-v1.json"
    second_surface_payload = {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_surface_learned_probability",
            "ranking_run_id": ranking_run_id,
            "candidate_pool_work_set_sha256": candidate_sha,
        },
        "candidate_work_scores": [
            {"canonical_openalex_work_id": "W1", "audit_embedding_probability_work": 0.1},
            {"canonical_openalex_work_id": "W2", "audit_embedding_probability_work": 0.2},
            {"canonical_openalex_work_id": "W3", "audit_embedding_probability_work": 0.3},
        ],
    }
    _write_json(tmp_path, "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json", second_surface_payload)

    found = second_surface._approved_probability_probe(
        repo_root=tmp_path,
        ranking_run_id=ranking_run_id,
        candidate_sha=candidate_sha,
        candidate_work_count=3,
    )

    assert found["probe_status"] == "found"
    assert found["source_artifact_path"] == "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json"
    assert found["learned_probability_coverage_count"] == 3
    assert found["full_coverage"] is True

    run_mismatch = second_surface._approved_probability_probe(
        repo_root=tmp_path,
        ranking_run_id="rank-other",
        candidate_sha=candidate_sha,
        candidate_work_count=3,
    )
    assert run_mismatch["probe_status"] == "not_found"

    sha_mismatch = second_surface._approved_probability_probe(
        repo_root=tmp_path,
        ranking_run_id=ranking_run_id,
        candidate_sha="different-sha",
        candidate_work_count=3,
    )
    assert sha_mismatch["probe_status"] == "not_found"

    second_surface_artifact.unlink()
    legacy_payload = {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_v1_audit_output",
            "ranking_run_id": "rank-legacy",
            "candidate_pool_work_set_sha256": "legacy-sha",
        },
        "shadow_output_rows": [
            {"canonical_openalex_work_id": "W10", "audit_embedding_probability_work": 0.4},
            {"canonical_openalex_work_id": "W11", "audit_embedding_probability_work": 0.5},
        ],
    }
    _write_json(tmp_path, "docs/audit/ml-shadow-scorer-v1-audit-output.json", legacy_payload)

    legacy = second_surface._approved_probability_probe(
        repo_root=tmp_path,
        ranking_run_id="rank-legacy",
        candidate_sha="legacy-sha",
        candidate_work_count=2,
    )
    assert legacy["probe_status"] == "found"
    assert legacy["source_artifact_path"] == "docs/audit/ml-shadow-scorer-v1-audit-output.json"
    assert legacy["learned_probability_coverage_count"] == 2


def test_blocked_no_candidate_source_meets_minimum(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _candidate_rows("G", 80)
    _install_query_fixtures(monkeypatch, [_source("rank-small")], {"rank-small": pool})

    payload = _build(tmp_path, monkeypatch)

    assert payload["discovery_summary"]["status"] == "blocked_no_candidate_source_meets_minimum"
    assert payload["recommended_next_stage"] == "create_or_expand_second_fresh_candidate_source_for_shadow_generalization_v1"


def test_blocked_database_unavailable_without_local_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    secret_dsn = "postgresql://admin:super-secret@localhost:5432/research"
    monkeypatch.setattr(second_surface, "_database_url_from_env", lambda: secret_dsn)
    monkeypatch.setattr(
        second_surface,
        "_connect_readonly",
        lambda database_url: (_ for _ in ()).throw(OSError(f"refused {database_url}")),
    )
    monkeypatch.setattr(second_surface, "_old_eval_ids_from_v3", lambda payload: {"WOLD001"})
    out_json = tmp_path / "out.json"
    out_md = tmp_path / "out.md"

    payload = write_ml_shadow_scorer_generalization_second_surface(
        **_paths(tmp_path),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=tmp_path,
    )

    assert payload["discovery_summary"]["status"] == "blocked_database_unavailable"
    assert payload["recommended_next_stage"] == "retry_local_database_discovery_for_shadow_generalization_v1"
    assert payload["metadata"]["database_unavailable_error"] == "OSError: details redacted"
    assert payload["metadata"]["database_target_redacted"] == (
        "postgresql://admin:***@localhost:5432/research"
    )
    assert secret_dsn not in json.dumps(payload)
    assert secret_dsn not in out_json.read_text(encoding="utf-8")
    assert secret_dsn not in out_md.read_text(encoding="utf-8")
    assert out_json.exists()
    assert out_md.exists()


def test_local_db_url_guard_rejects_hosted_prod_urls() -> None:
    with pytest.raises(MLShadowScorerGeneralizationSecondSurfaceError, match="host"):
        assert_local_database_url("postgresql://user:pass@prod.neon.tech/db")


def test_select_only_guard_rejects_writes() -> None:
    class Cursor:
        def execute(self, sql: str, params: tuple = ()) -> None:
            raise AssertionError("should not execute")

    with pytest.raises(MLShadowScorerGeneralizationSecondSurfaceError, match="SELECT"):
        second_surface._execute_select(Cursor(), "UPDATE paper_scores SET final_score = 0")


def test_label_dataset_version_check_uses_top_level_or_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _candidate_rows("H", 120)
    labels = _label_dataset_payload([row["canonical_openalex_work_id"] for row in pool])
    del labels["dataset_version"]
    _install_query_fixtures(
        monkeypatch,
        [_source("rank-metadata-label-version")],
        {"rank-metadata-label-version": pool},
        probability_full_for={"rank-metadata-label-version"},
    )

    payload = _build(tmp_path, monkeypatch, labels=labels)

    assert payload["metadata"]["source_label_dataset_version"] == "ml-label-dataset-v11"


def test_cli_writes_blocked_database_unavailable_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(second_surface, "_connect_readonly", lambda database_url: (_ for _ in ()).throw(OSError("refused")))
    monkeypatch.setattr(second_surface, "_old_eval_ids_from_v3", lambda payload: {"WOLD001"})
    paths = _paths(tmp_path)
    out_json = tmp_path / "second-surface.json"
    out_md = tmp_path / "second-surface.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-generalization-second-surface",
        "--generalization-audit-plan",
        str(paths["generalization_audit_plan_path"]),
        "--online-shadow-policy",
        str(paths["online_shadow_policy_path"]),
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
    assert data["metadata"]["surface_version"] == SURFACE_VERSION
    assert data["discovery_summary"]["status"] == "blocked_database_unavailable"
    assert "Generalization Second Surface" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_forbidden_ml_clients_and_shadow_prod_false() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_generalization_second_surface.py").read_text(
        encoding="utf-8"
    ).lower()
    test_source = Path(__file__).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    for forbidden in ("openai", "openalex", "sklearn"):
        assert forbidden not in import_lines
