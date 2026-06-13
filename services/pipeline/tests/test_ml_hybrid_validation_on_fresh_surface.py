"""Tests for fresh-surface hybrid validation v1."""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_hybrid_validation_on_fresh_surface import (
    PRIMARY_CONFIRMATORY_ARM,
    VALIDATION_VERSION,
    MLHybridValidationOnFreshSurfaceError,
    build_ml_hybrid_validation_on_fresh_surface_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


class _FakeCur:
    def __init__(self, parent: "_FakeConn") -> None:
        self._parent = parent
        self._sql = ""

    def execute(self, query: str, params: tuple | None = None) -> "_FakeCur":
        self._sql = query
        self._parent.executed_sql.append(query)
        self._parent.executed_params.append(params or ())
        return self

    def fetchall(self) -> list[dict]:
        if "FROM works w" in self._sql and "JOIN embeddings e" in self._sql:
            return [
                {"internal_work_id": work_id, "vector": json.dumps(vector)}
                for work_id, vector in self._parent.vectors_by_internal_id.items()
            ]
        return []


class _FakeCurCtx:
    def __init__(self, parent: "_FakeConn") -> None:
        self._cur = _FakeCur(parent)

    def __enter__(self) -> _FakeCur:
        return self._cur

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, vectors_by_internal_id: dict[int, list[float]] | None = None) -> None:
        self.vectors_by_internal_id = vectors_by_internal_id or _vectors()
        self.executed_sql: list[str] = []
        self.executed_params: list[tuple] = []

    def cursor(self, row_factory: object | None = None) -> _FakeCurCtx:
        return _FakeCurCtx(self)


EXPECTED_ARMS = {
    "heuristic_final_score_baseline",
    "holdout_embedding_probability_baseline",
    "hybrid_rank_mean_50_50",
    "hybrid_rank_mean_75_25_heuristic",
    "hybrid_rank_mean_25_75_heuristic",
}


def _vectors() -> dict[int, list[float]]:
    return {
        100: [0.0, 0.0],
        1: [2.0, 0.0],
        2: [-2.0, 0.0],
        3: [1.0, 0.0],
        4: [-1.0, 0.0],
        101: [0.0, 0.0],
    }


def _candidate(work_id: str, internal_id: int, score: float, rank: int, *, confirmatory: bool) -> dict:
    return {
        "canonical_openalex_work_id": work_id,
        "openalex_id": f"https://openalex.org/{work_id}",
        "internal_work_id": internal_id,
        "title": f"Work {work_id}",
        "year": 2026,
        "citation_count": rank,
        "ranking_run_id": "rank-9f4b2a2084",
        "family": "emerging",
        "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
        "heuristic_rank": rank,
        "final_score": score,
        "confirmatory_metric_eligible": confirmatory,
        "previous_eval_overlap": not confirmatory,
        "duplicate_candidate_canonical_work": False,
    }


def _surface_payload() -> dict:
    rows = [
        _candidate("W0", 100, 0.99, 1, confirmatory=False),
        _candidate("W1", 1, 0.90, 2, confirmatory=True),
        _candidate("W2", 2, 0.80, 3, confirmatory=True),
        _candidate("W3", 3, 0.70, 4, confirmatory=True),
        _candidate("W4", 4, 0.60, 5, confirmatory=True),
        _candidate("W5", 101, 0.10, 6, confirmatory=False),
    ]
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
            "status": "materialized_ready",
            "label_dataset_version": "ml-label-dataset-v10",
            "expected_label_dataset_version": "ml-label-dataset-v10",
        },
        "candidate_source": {
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
        },
        "candidate_pool": {
            "candidate_work_count": len(rows),
            "candidate_work_set_sha256": "candidate-sha",
            "candidate_rows": rows,
        },
        "confirmatory_eligibility": {
            "confirmatory_metric_eligible_work_count": 4,
            "excluded_previous_eval_overlap_count": 2,
        },
        "disallowed_overlap_report": {"overlap_work_count": 2},
        "label_coverage": {
            "work_level": {
                "confirmatory_candidate_work_count": 4,
                "confirmatory_labeled_work_count": 4,
                "confirmatory_positive_work_count": 2,
                "confirmatory_negative_work_count": 2,
                "distinct_negative_work_count": 2,
            }
        },
        "threshold_check": {
            "minimum_candidate_work_count": {"observed": 4, "threshold": 4, "passed": True},
            "minimum_confirmatory_labeled_work_count": {"observed": 4, "threshold": 4, "passed": True},
            "minimum_confirmatory_positive_work_count": {"observed": 2, "threshold": 2, "passed": True},
            "minimum_confirmatory_negative_work_count": {"observed": 2, "threshold": 2, "passed": True},
            "minimum_confirmatory_label_coverage_rate": {"observed": 1.0, "threshold": 0.6, "passed": True},
            "minimum_distinct_negative_work_count": {"observed": 2, "threshold": 2, "passed": True},
        },
        "ready_for_hybrid_validation_scoring": True,
        "recommended_next_stage": "execute_hybrid_validation_on_fresh_surface_v1",
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
        "gate_linkage": {
            "material_lift_thresholds": {
                "delta_roc_auc_gte": 0.03,
                "or_delta_average_precision_gte": 0.02,
            }
        },
    }


def _label_row(row_id: str, work_id: str, target: bool) -> dict:
    return {
        "row_id": row_id,
        "paper_id": f"https://openalex.org/{work_id}",
        "work_id": work_id,
        "openalex_work_id": work_id,
        "relevance_label": "good" if target else "irrelevant",
        "novelty_label": "useful" if target else "obvious",
        "bridge_like_label": "partial" if target else "no",
        "good_or_acceptable": target,
    }


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v10",
        "rows": [
            _label_row("r1", "W1", True),
            _label_row("r2", "W2", False),
            _label_row("r3", "W3", True),
            _label_row("r4", "W4", False),
            _label_row("overlap", "W0", True),
        ],
    }


def _scorer_payload(**metadata_overrides: object) -> dict:
    metadata = {
        "artifact_type": "ml_offline_audit_embedding_scorer",
        "scorer_version": "ml-offline-audit-embedding-scorer-v2",
        "fit_mode": "holdout_bound_train_only",
        "embedding_dimensions": 2,
    }
    metadata.update(metadata_overrides)
    return {
        "metadata": metadata,
        "policy_compliance": {"eval_works_excluded_from_fit": True},
        "scorer": {
            "scaler": {"mean": [0.0, 0.0], "scale": [1.0, 1.0], "feature_count": 2},
            "classifier": {
                "coefficients_standardized_space": [1.0, 0.0],
                "intercept_standardized_space": 0.0,
            },
        },
    }


def _embeddings_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_hybrid_snapshot_embeddings",
            "artifact_version": "ml-fresh-hybrid-snapshot-embeddings-v1",
            "snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
            "embedding_version": "fresh-hybrid-text-embedding-v1",
        },
        "embedding_result": {
            "status": "succeeded",
            "full_snapshot_embedding_coverage": True,
        },
    }


def _spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_offline_experiment_spec",
            "spec_version": "ml-hybrid-scorer-offline-experiment-v1-spec",
        },
        "pre_registered_hybrid_arms": [
            {"arm_id": "heuristic_final_score_baseline", "score_formula": "final_score"},
            {"arm_id": "holdout_embedding_probability_baseline", "score_formula": "audit_embedding_probability_work"},
            {
                "arm_id": "hybrid_rank_mean_50_50",
                "score_formula": "0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
            },
            {
                "arm_id": "hybrid_rank_mean_75_25_heuristic",
                "score_formula": "0.75 * rank_pct(final_score) + 0.25 * rank_pct(audit_embedding_probability_work)",
            },
            {
                "arm_id": "hybrid_rank_mean_25_75_heuristic",
                "score_formula": "0.25 * rank_pct(final_score) + 0.75 * rank_pct(audit_embedding_probability_work)",
            },
        ],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    surface: dict | None = None,
    policy: dict | None = None,
    labels: dict | None = None,
    scorer: dict | None = None,
    embeddings: dict | None = None,
) -> dict[str, Path]:
    return {
        "fresh_eval_surface_path": _write_json(tmp_path, "surface.json", surface or _surface_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", labels or _label_payload()),
        "audit_embedding_scorer_export_path": _write_json(tmp_path, "scorer.json", scorer or _scorer_payload()),
        "fresh_hybrid_snapshot_embeddings_path": _write_json(tmp_path, "embeddings.json", embeddings or _embeddings_payload()),
        "hybrid_experiment_spec_path": _write_json(tmp_path, "spec.json", _spec_payload()),
    }


def _build(tmp_path: Path, conn: _FakeConn | None = None, **kwargs: object) -> dict:
    return build_ml_hybrid_validation_on_fresh_surface_payload(
        conn or _FakeConn(),
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-20T00:00:00Z",
    )


def test_happy_path_with_mocked_db_vectors(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn)

    assert payload["metadata"]["artifact_type"] == "ml_hybrid_validation_on_fresh_surface"
    assert payload["metadata"]["validation_version"] == VALIDATION_VERSION
    assert payload["validation_scope"]["candidate_pool_work_count"] == 6
    assert payload["validation_scope"]["confirmatory_metric_work_count"] == 4
    assert set(payload["arm_metrics"]) == EXPECTED_ARMS
    assert payload["embedding_join_summary"]["embedded_work_count"] == 6
    assert payload["label_join_summary"]["positive_work_count"] == 2
    assert payload["summary"]["recommended_next_stage"] == "run_hybrid_validation_metric_gates_v1"
    assert all(not re.search(r"\b(insert|update|delete|drop|alter|create|truncate|merge)\b", sql, re.I) for sql in conn.executed_sql)


def test_rank_percentiles_use_full_pool_length(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    by_work = {row["canonical_openalex_work_id"]: row for row in payload["candidate_work_scores"]}

    assert by_work["W1"]["final_score_rank_pct"] == pytest.approx(0.8)
    assert by_work["W1"]["final_score_rank_pct"] < 1.0


def test_metrics_computed_only_on_confirmatory_labeled_rows(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    for metrics in payload["arm_metrics"].values():
        assert metrics["labeled_eval_subset_work_count"] == 4
        assert metrics["positive_work_count"] == 2
        assert metrics["negative_work_count"] == 2


def test_primary_arm_stays_fixed_even_if_other_arm_is_best(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["confirmatory_decision_inputs"]["primary_confirmatory_arm"] == PRIMARY_CONFIRMATORY_ARM
    assert payload["confirmatory_decision_inputs"]["best_arm_selection_is_exploratory_only"] is True
    assert payload["confirmatory_decision_inputs"]["confirmatory_validation_passed"] is False


def test_rejects_ready_false(tmp_path: Path) -> None:
    surface = _surface_payload()
    surface["ready_for_hybrid_validation_scoring"] = False
    with pytest.raises(MLHybridValidationOnFreshSurfaceError, match="ready_for_hybrid_validation_scoring"):
        _build(tmp_path, surface=surface)


def test_rejects_failed_threshold(tmp_path: Path) -> None:
    surface = _surface_payload()
    surface["threshold_check"]["minimum_candidate_work_count"]["passed"] = False
    with pytest.raises(MLHybridValidationOnFreshSurfaceError, match="threshold_check"):
        _build(tmp_path, surface=surface)


@pytest.mark.parametrize(
    ("scorer", "message"),
    [
        (_scorer_payload(scorer_version="wrong"), "scorer_version"),
        (_scorer_payload(fit_mode="full_fit_audit_corpus"), "fit_mode"),
        ({**_scorer_payload(), "policy_compliance": {"eval_works_excluded_from_fit": False}}, "eval_works_excluded"),
    ],
)
def test_rejects_scorer_version_fit_mode_policy_failures(tmp_path: Path, scorer: dict, message: str) -> None:
    with pytest.raises(MLHybridValidationOnFreshSurfaceError, match=message):
        _build(tmp_path, scorer=scorer)


def test_rejects_missing_embedding_for_any_pool_work(tmp_path: Path) -> None:
    vectors = _vectors()
    vectors.pop(4)
    with pytest.raises(MLHybridValidationOnFreshSurfaceError, match="missing embeddings"):
        _build(tmp_path, _FakeConn(vectors))


def test_rejects_confirmatory_previous_eval_overlap_true(tmp_path: Path) -> None:
    surface = _surface_payload()
    surface["candidate_pool"]["candidate_rows"][1]["previous_eval_overlap"] = True
    with pytest.raises(MLHybridValidationOnFreshSurfaceError, match="previous_eval_overlap"):
        _build(tmp_path, surface=surface)


def test_rejects_label_conflicts(tmp_path: Path) -> None:
    labels = _label_payload()
    labels["rows"].append(_label_row("r1-conflict", "W1", False))
    with pytest.raises(MLHybridValidationOnFreshSurfaceError, match="conflicting target"):
        _build(tmp_path, labels=labels)


def test_rejects_unlabeled_confirmatory_works(tmp_path: Path) -> None:
    labels = _label_payload()
    labels["rows"] = [row for row in labels["rows"] if row["work_id"] != "W4"]
    with pytest.raises(MLHybridValidationOnFreshSurfaceError, match="unlabeled confirmatory"):
        _build(tmp_path, labels=labels)


def test_rejects_hosted_database_url(tmp_path: Path) -> None:
    with pytest.raises(MLHybridValidationOnFreshSurfaceError, match="hosted production"):
        build_ml_hybrid_validation_on_fresh_surface_payload(
            _FakeConn(),
            **_paths(tmp_path),
            database_url="postgresql://user:pass@containers-us-west-1.railway.app:5432/railway",
            repo_root=tmp_path,
        )


def test_cli_parser_registers_command() -> None:
    cli_source = read_cli_parser_source(PACKAGE_ROOT)
    assert "ml-hybrid-validation-on-fresh-surface" in cli_source


def test_new_module_has_no_openai_openalex_client_or_sklearn_imports() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_hybrid_validation_on_fresh_surface.py").read_text(
        encoding="utf-8"
    ).lower()
    test_source = Path(__file__).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    assert "sklearn" not in import_lines
    assert "openai" not in import_lines
    assert "pipeline.openalex" not in import_lines
