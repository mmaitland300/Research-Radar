"""Tests for second-surface learned-probability application."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.ml_shadow_scorer_second_surface_learned_probability_apply as apply_module
from pipeline.ml_shadow_scorer_second_surface_learned_probability_apply import (
    DEFAULT_CORPUS_SNAPSHOT_VERSION,
    DEFAULT_EMBEDDING_VERSION,
    DEFAULT_FAMILY,
    DEFAULT_RANKING_RUN_ID,
    EXPECTED_CANDIDATE_POOL_WORK_COUNT,
    EXPECTED_CANDIDATE_SHA,
    MLShadowScorerSecondSurfaceLearnedProbabilityApplyError,
    build_ml_shadow_scorer_second_surface_learned_probability_apply_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_surface_learned_probability_coverage_plan",
            "plan_version": "ml-shadow-scorer-v1-second-surface-learned-probability-coverage-plan-v1",
        },
        "recommended_next_stage": "apply_second_surface_learned_probability_coverage_v1",
        "learned_probability_coverage_contract": {
            "approved_scorer": "ml-offline-audit-embedding-scorer-v2",
            "approved_embedding_version": DEFAULT_EMBEDDING_VERSION,
            "approved_embeddings_artifact": "docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json",
            "must_not_refit": True,
            "must_not_regenerate_embeddings": True,
            "must_not_use_v11_labels_as_scorer_features": True,
            "output_field": "audit_embedding_probability_work",
            "target_coverage": {
                "covered_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
                "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
                "coverage_rate": 1.0,
            },
        },
    }


def _discovery_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_second_surface",
            "surface_version": "ml-shadow-scorer-v1-generalization-second-surface-v1",
        },
        "discovery_summary": {"status": "selected_needs_learned_probability_coverage"},
        "selected_second_surface": {
            "ranking_run_id": DEFAULT_RANKING_RUN_ID,
            "family": DEFAULT_FAMILY,
            "corpus_snapshot_version": DEFAULT_CORPUS_SNAPSHOT_VERSION,
            "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
            "confirmatory_metric_eligible_work_count": 168,
        },
        "label_coverage": {
            "work_level": {
                "confirmatory_labeled_work_count": 168,
                "confirmatory_positive_work_count": 94,
                "confirmatory_negative_work_count": 74,
            }
        },
        "learned_probability_coverage": {
            "learned_probability_coverage_count": 0,
            "missing_learned_probability_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "approved_upstream_probability_probe": {"probe_status": "not_found"},
        },
    }


def _label_dataset_payload(*, rows_label: str = "good", version: str = "ml-label-dataset-v11") -> dict:
    return {
        "dataset_version": version,
        "metadata": {
            "dataset_version": version,
            "shadow_generalization_second_surface_v1_ingest": {
                "ranking_run_id": DEFAULT_RANKING_RUN_ID,
                "family": DEFAULT_FAMILY,
                "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
                "labeled_count": 168,
                "positive_count": 94,
                "negative_count": 74,
                "label_thresholds_passed": True,
            },
        },
        "rows": [
            {
                "work_id": "W000000001",
                "relevance_label": rows_label,
                "good_or_acceptable": rows_label in {"good", "acceptable"},
            }
        ],
    }


def _embeddings_payload(*, embedded: int = EXPECTED_CANDIDATE_POOL_WORK_COUNT) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_snapshot_embeddings",
            "artifact_version": "ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
            "snapshot_version": DEFAULT_CORPUS_SNAPSHOT_VERSION,
            "embedding_version": DEFAULT_EMBEDDING_VERSION,
        },
        "coverage": {
            "snapshot_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "embedded_work_count": embedded,
            "missing_embedding_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT - embedded,
        },
    }


def _scorer_payload(*, artifact_type: str = "ml_offline_audit_embedding_scorer", version: str = "ml-offline-audit-embedding-scorer-v2", fit_mode: str = "holdout_bound_train_only", target: str = "good_or_acceptable", dimensions: int = 2) -> dict:
    return {
        "metadata": {
            "artifact_type": artifact_type,
            "scorer_version": version,
            "fit_mode": fit_mode,
            "target": target,
            "embedding_dimensions": dimensions,
        },
        "scorer": {
            "scaler": {"feature_count": dimensions, "mean": [0.0] * dimensions, "scale": [1.0] * dimensions},
            "classifier": {
                "coefficients_standardized_space": [1.0] + [-0.5] * (dimensions - 1),
                "intercept_standardized_space": 0.0,
            },
        },
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    plan: dict | None = None,
    discovery: dict | None = None,
    labels: dict | None = None,
    embeddings: dict | None = None,
    scorer: dict | None = None,
) -> dict[str, Path]:
    return {
        "learned_probability_coverage_plan_path": _write_json(tmp_path, "plan.json", plan or _plan_payload()),
        "generalization_second_surface_path": _write_json(tmp_path, "discovery.json", discovery or _discovery_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", labels or _label_dataset_payload()),
        "second_snapshot_embeddings_path": _write_json(tmp_path, "embeddings.json", embeddings or _embeddings_payload()),
        "offline_audit_embedding_scorer_path": _write_json(tmp_path, "scorer.json", scorer or _scorer_payload()),
    }


def _candidate_rows(*, count: int = EXPECTED_CANDIDATE_POOL_WORK_COUNT, dimensions: int = 2) -> list[dict]:
    rows = []
    for index in range(1, count + 1):
        work_id = f"W99{index:07d}"
        vector = [index / 1000.0, (count - index) / 1000.0]
        if dimensions != 2:
            vector = [index / 1000.0 for _ in range(dimensions)]
        rows.append(
            {
                "ranking_run_id": DEFAULT_RANKING_RUN_ID,
                "internal_work_id": index,
                "recommendation_family": DEFAULT_FAMILY,
                "final_score": float(count - index),
                "openalex_id": f"https://openalex.org/{work_id}",
                "title": f"Candidate {index}",
                "year": 2020 + (index % 5),
                "corpus_snapshot_version": DEFAULT_CORPUS_SNAPSHOT_VERSION,
                "vector": json.dumps(vector) if index % 2 == 0 else vector,
            }
        )
    return rows


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn
        self.rows: list[dict] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.conn.executed_sql.append(sql)
        lowered = sql.lower()
        if "from ranking_runs" in lowered:
            self.rows = [self.conn.ranking_row]
        elif "from paper_scores" in lowered:
            self.rows = list(self.conn.candidate_rows)
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> dict | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[dict]:
        return list(self.rows)


class _FakeConn:
    def __init__(self, candidate_rows: list[dict] | None = None) -> None:
        self.ranking_row = {
            "ranking_run_id": DEFAULT_RANKING_RUN_ID,
            "status": "succeeded",
            "ranking_version": "shadow-generalization-product-candidate-ranking-v1",
            "corpus_snapshot_version": DEFAULT_CORPUS_SNAPSHOT_VERSION,
            "embedding_version": DEFAULT_EMBEDDING_VERSION,
        }
        self.candidate_rows = candidate_rows or _candidate_rows()
        self.executed_sql: list[str] = []
        self.closed = False

    def cursor(self, row_factory: object = None) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True


def _build(tmp_path: Path, *, conn: _FakeConn | None = None, **kwargs: object) -> dict:
    monkeypatch_sha = kwargs.pop("monkeypatch_sha", True)
    if monkeypatch_sha:
        with patch.object(apply_module, "_work_set_sha256", return_value=EXPECTED_CANDIDATE_SHA):
            return build_ml_shadow_scorer_second_surface_learned_probability_apply_payload(
                conn or _FakeConn(),
                **_paths(tmp_path, **kwargs),
                repo_root=tmp_path,
            )
    return build_ml_shadow_scorer_second_surface_learned_probability_apply_payload(
        conn or _FakeConn(),
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
    )


def test_happy_path_computes_probabilities_for_all_rows(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn=conn)

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_surface_learned_probability"
    assert payload["execution_summary"]["status"] == "succeeded"
    assert payload["execution_summary"]["learned_probability_coverage_count"] == EXPECTED_CANDIDATE_POOL_WORK_COUNT
    assert payload["execution_summary"]["missing_learned_probability_count"] == 0
    assert payload["coverage_summary"]["embedding_coverage_count"] == EXPECTED_CANDIDATE_POOL_WORK_COUNT
    assert len(payload["candidate_work_scores"]) == EXPECTED_CANDIDATE_POOL_WORK_COUNT
    assert all(row["audit_embedding_probability_work"] is not None for row in payload["candidate_work_scores"])
    assert payload["metadata"]["labels_used_for_scoring"] is False
    assert payload["metadata"]["db_writes_performed"] is False
    assert payload["recommended_next_stage"] == "extend_second_surface_probability_probe_and_rerun_discovery_v1"


def test_rejects_wrong_plan_artifact_type_or_next_stage(tmp_path: Path) -> None:
    plan = _plan_payload()
    plan["metadata"]["artifact_type"] = "wrong"
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityApplyError, match="artifact_type"):
        _build(tmp_path / "artifact", plan=plan)

    plan = _plan_payload()
    plan["recommended_next_stage"] = "wrong"
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityApplyError, match="recommended_next_stage"):
        _build(tmp_path / "next", plan=plan)


def test_rejects_discovery_status_or_provenance_mismatch(tmp_path: Path) -> None:
    discovery = _discovery_payload()
    discovery["discovery_summary"]["status"] = "selected_needs_labels"
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityApplyError, match="status"):
        _build(tmp_path / "status", discovery=discovery)

    for key, value in (
        ("ranking_run_id", "rank-other"),
        ("family", "bridge"),
        ("corpus_snapshot_version", "other-snapshot"),
        ("candidate_pool_work_set_sha256", "bad"),
    ):
        discovery = _discovery_payload()
        discovery["selected_second_surface"][key] = value
        with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityApplyError, match=key):
            _build(tmp_path / key, discovery=discovery)


def test_rejects_incomplete_embeddings(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityApplyError, match="embedded_work_count"):
        _build(tmp_path, embeddings=_embeddings_payload(embedded=527))


def test_rejects_wrong_scorer_metadata_or_dimensions(tmp_path: Path) -> None:
    for name, scorer, match in (
        ("type", _scorer_payload(artifact_type="wrong"), "artifact_type"),
        ("version", _scorer_payload(version="v1"), "scorer_version"),
        ("fit", _scorer_payload(fit_mode="full_fit_audit_corpus"), "fit_mode"),
        ("target", _scorer_payload(target="surprising_or_useful"), "target"),
        ("dimensions", _scorer_payload(dimensions=3), "embedding vector length"),
    ):
        with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityApplyError, match=match):
            _build(tmp_path / name, scorer=scorer)


def test_rejects_hosted_prod_database_url(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityApplyError, match="host"):
        build_ml_shadow_scorer_second_surface_learned_probability_apply_payload(
            _FakeConn(),
            **_paths(tmp_path),
            database_url="postgresql://user:pass@prod.neon.tech/db",
            repo_root=tmp_path,
        )


def test_labels_are_not_used_for_scoring(tmp_path: Path) -> None:
    payload_a = _build(tmp_path / "a", labels=_label_dataset_payload(rows_label="good"))
    payload_b = _build(tmp_path / "b", labels=_label_dataset_payload(rows_label="irrelevant"))

    scores_a = [
        row["audit_embedding_probability_work"]
        for row in payload_a["candidate_work_scores"][:10]
    ]
    scores_b = [
        row["audit_embedding_probability_work"]
        for row in payload_b["candidate_work_scores"][:10]
    ]
    assert scores_a == scores_b


def test_no_write_sql_statements_are_executed(tmp_path: Path) -> None:
    conn = _FakeConn()
    _build(tmp_path, conn=conn)

    sql_text = "\n".join(conn.executed_sql).lower()
    for forbidden in ("insert", "update", "delete", "drop", "alter", "create", "truncate"):
        assert forbidden not in sql_text

    with pytest.raises(MLShadowScorerSecondSurfaceLearnedProbabilityApplyError, match="SELECT"):
        apply_module._execute_select(_FakeCursor(conn), "UPDATE paper_scores SET final_score = 0")


def test_candidate_work_scores_are_deterministically_sorted(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    rows = payload["candidate_work_scores"]
    sort_keys = [
        (
            -row["audit_embedding_probability_work"],
            -row["final_score"],
            row["canonical_openalex_work_id"],
        )
        for row in rows
    ]
    assert sort_keys == sorted(sort_keys)


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "out.json"
    out_md = tmp_path / "out.md"
    fake_conn = _FakeConn()
    monkeypatch.setattr(apply_module, "_database_url_from_env", lambda: "postgresql://research_radar:research_radar@localhost:5432/research_radar")
    monkeypatch.setattr(apply_module, "_connect_readonly", lambda database_url: fake_conn)
    monkeypatch.setattr(apply_module, "_work_set_sha256", lambda work_ids: EXPECTED_CANDIDATE_SHA)

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-second-surface-learned-probability-apply",
        "--learned-probability-coverage-plan",
        str(paths["learned_probability_coverage_plan_path"]),
        "--generalization-second-surface",
        str(paths["generalization_second_surface_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--second-snapshot-embeddings",
        str(paths["second_snapshot_embeddings_path"]),
        "--offline-audit-embedding-scorer",
        str(paths["offline_audit_embedding_scorer_path"]),
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
    assert payload["execution_summary"]["learned_probability_coverage_count"] == EXPECTED_CANDIDATE_POOL_WORK_COUNT
    assert "Top 20 Probability Preview" in out_md.read_text(encoding="utf-8")


def test_no_direct_forbidden_imports_and_cli_has_expected_database_url() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_surface_learned_probability_apply.py"
    ).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in module_source.splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    for forbidden in ("openai", "openalex", "sklearn"):
        assert forbidden not in import_lines
    assert "fit(" not in module_source
    assert "logisticregression" not in module_source

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-second-surface-learned-probability-apply"')
    end = cli_source.index("ml_shadow_scorer_second_candidate_plan_ingest_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" in parser_block
