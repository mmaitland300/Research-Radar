"""Read-only serving helper for the bounded Bridge rank-pct hybrid scorer."""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_offline_bridge_hybrid_eval_v3 import SELECTED_FROZEN_C, _as_float
from pipeline.ml_offline_bridge_hybrid_rank_pct_eval_v3 import (
    _compute_rank_percentiles_from_pool,
    _score_pool_ml_probabilities,
)
from pipeline.openalex_ids import normalize_w_token
from pipeline.repo_paths import default_repo_root

ARTIFACT_TYPE = "ml_bridge_rank_pct_hybrid_serving_plan"
PLAN_VERSION = "ml-bridge-rank-pct-hybrid-serving-plan-v1"
DEFAULT_SERVING_PLAN_PATH = Path("docs/audit/ml-bridge-rank-pct-hybrid-serving-plan-v1.json")
OPENALEX_WORK_URL_PREFIX = "https://openalex.org/"
PINNED_RANKING_RUN_ID = "rank-5a7efa5ca3"
FAMILY = "bridge"
PRIMARY_ALPHA = 0.5
RANK_PCT_SCOPE = "full_bridge_candidate_pool"
EXPECTED_CANDIDATE_COUNT = 528
SCORER_SERVED_LIMIT = 20
WRITE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|merge|grant|revoke|vacuum|reindex|copy)\b"
)


class MLBridgeScorerRolloutServingError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLBridgeScorerRolloutServingError(f"failed to load {label} JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLBridgeScorerRolloutServingError(f"{label} JSON must be an object: {path}")
    return payload


def _resolve_repo_path(raw: str, *, root: Path) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else root / path


def _validate_sha256(path: Path, expected: Any, *, label: str) -> None:
    if not isinstance(expected, str) or not expected.strip():
        raise MLBridgeScorerRolloutServingError(f"serving plan missing {label} SHA256")
    raw = path.read_bytes()
    raw_actual = hashlib.sha256(raw).hexdigest()
    normalized_actual = raw_actual
    if b"\r\n" in raw:
        normalized_actual = hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()
    if expected not in {raw_actual, normalized_actual}:
        raise MLBridgeScorerRolloutServingError(
            f"{label} SHA256 mismatch for {path}: expected {expected}, "
            f"got {raw_actual} (raw), {normalized_actual} (lf-normalized)"
        )


def _validate_serving_plan(payload: Mapping[str, Any], *, path: Path) -> None:
    if payload.get("artifact_type") != ARTIFACT_TYPE:
        raise MLBridgeScorerRolloutServingError(
            f"{path} artifact_type={payload.get('artifact_type')!r}; expected {ARTIFACT_TYPE!r}"
        )
    if payload.get("plan_version") != PLAN_VERSION:
        raise MLBridgeScorerRolloutServingError(
            f"{path} plan_version={payload.get('plan_version')!r}; expected {PLAN_VERSION!r}"
        )
    if payload.get("ranking_run_id") != PINNED_RANKING_RUN_ID:
        raise MLBridgeScorerRolloutServingError(
            f"{path} ranking_run_id={payload.get('ranking_run_id')!r}; expected {PINNED_RANKING_RUN_ID!r}"
        )
    if _as_float(payload.get("selected_frozen_coefficient_C")) != SELECTED_FROZEN_C:
        raise MLBridgeScorerRolloutServingError(
            f"{path} selected_frozen_coefficient_C must be {SELECTED_FROZEN_C}"
        )
    if _as_float(payload.get("primary_alpha")) != PRIMARY_ALPHA:
        raise MLBridgeScorerRolloutServingError(f"{path} primary_alpha must be {PRIMARY_ALPHA}")
    if payload.get("rank_pct_scope") != RANK_PCT_SCOPE:
        raise MLBridgeScorerRolloutServingError(
            f"{path} rank_pct_scope={payload.get('rank_pct_scope')!r}; expected {RANK_PCT_SCOPE!r}"
        )


def _validate_frozen_scorer(payload: Mapping[str, Any], *, path: Path) -> Mapping[str, Any]:
    if _as_float(payload.get("selected_frozen_coefficient_C")) != SELECTED_FROZEN_C:
        raise MLBridgeScorerRolloutServingError(
            f"{path} selected_frozen_coefficient_C must be {SELECTED_FROZEN_C}"
        )
    frozen = payload.get("selected_frozen_scorer")
    if not isinstance(frozen, Mapping) or not frozen:
        raise MLBridgeScorerRolloutServingError(f"{path} missing selected_frozen_scorer object")
    if _as_float(frozen.get("C")) != SELECTED_FROZEN_C:
        raise MLBridgeScorerRolloutServingError(
            f"{path} selected_frozen_scorer.C must be {SELECTED_FROZEN_C}"
        )
    for key in ("scaler_mean", "scaler_scale", "coef", "intercept", "embedding_version"):
        if key not in frozen:
            raise MLBridgeScorerRolloutServingError(f"{path} selected_frozen_scorer missing {key!r}")
    return frozen


def _load_serving_plan_and_frozen_scorer(
    *,
    root: Path,
    serving_plan_path: Path | None,
) -> tuple[dict[str, Any], Mapping[str, Any]]:
    plan_path = (root / DEFAULT_SERVING_PLAN_PATH) if serving_plan_path is None else serving_plan_path
    if not plan_path.is_absolute():
        plan_path = root / plan_path
    plan_payload = _load_json_object(plan_path, label="Bridge serving plan")
    _validate_serving_plan(plan_payload, path=plan_path)
    frozen_contract = plan_payload.get("frozen_scorer_load_contract")
    if not isinstance(frozen_contract, Mapping):
        raise MLBridgeScorerRolloutServingError(f"{plan_path} missing frozen_scorer_load_contract")
    sensitivity_raw = frozen_contract.get("sensitivity_artifact_path")
    if not isinstance(sensitivity_raw, str) or not sensitivity_raw.strip():
        raise MLBridgeScorerRolloutServingError(f"{plan_path} missing sensitivity artifact path")
    sensitivity_path = _resolve_repo_path(sensitivity_raw, root=root)
    _validate_sha256(
        sensitivity_path,
        frozen_contract.get("sensitivity_artifact_sha256"),
        label="sensitivity artifact",
    )
    embeddings_raw = frozen_contract.get("embeddings_provenance_path")
    if not isinstance(embeddings_raw, str) or not embeddings_raw.strip():
        raise MLBridgeScorerRolloutServingError(f"{plan_path} missing embeddings provenance path")
    embeddings_path = _resolve_repo_path(embeddings_raw, root=root)
    _validate_sha256(
        embeddings_path,
        frozen_contract.get("embeddings_provenance_sha256"),
        label="embeddings provenance",
    )
    sensitivity_payload = _load_json_object(sensitivity_path, label="Bridge sensitivity artifact")
    frozen_scorer = _validate_frozen_scorer(sensitivity_payload, path=sensitivity_path)
    if frozen_contract.get("embedding_version") != frozen_scorer.get("embedding_version"):
        raise MLBridgeScorerRolloutServingError("serving plan embedding_version does not match frozen scorer")
    return plan_payload, frozen_scorer


def _execute_select(cur: Any, sql: str, params: Sequence[Any] | None = None) -> Any:
    lowered = sql.strip().lower()
    if not lowered.startswith("select"):
        raise MLBridgeScorerRolloutServingError("DB safety violation: SQL must start with SELECT")
    if WRITE_SQL_RE.search(lowered):
        raise MLBridgeScorerRolloutServingError("DB safety violation: SQL contains write/DDL verb")
    return cur.execute(sql, tuple(params or ()))


def _fetch_bridge_candidates_from_db(
    database_url: str,
    *,
    ranking_run_id: str = PINNED_RANKING_RUN_ID,
) -> list[dict[str, Any]]:
    query = """
        SELECT
            ps.work_id AS work_id_int,
            w.openalex_id,
            ps.bridge_score,
            ps.final_score,
            ROW_NUMBER() OVER (
                ORDER BY ps.final_score DESC, ps.work_id ASC
            ) AS current_family_rank
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = %s
        ORDER BY ps.final_score DESC, ps.work_id ASC
    """
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as conn:
            conn.execute("SET default_transaction_read_only = on")
            with conn.cursor(row_factory=dict_row) as cur:
                _execute_select(cur, query.strip(), (ranking_run_id, FAMILY))
                rows = cur.fetchall()
    except Exception as exc:
        raise MLBridgeScorerRolloutServingError(f"Bridge candidate DB read failed: {type(exc).__name__}") from exc
    if not rows:
        raise MLBridgeScorerRolloutServingError(
            f"no Bridge candidates found for ranking_run_id={ranking_run_id!r}"
        )
    return [dict(row) for row in rows]


def _candidate_token(row: Mapping[str, Any]) -> str:
    token = normalize_w_token(str(row.get("work_id_token") or row.get("openalex_id") or ""))
    if not token:
        raise MLBridgeScorerRolloutServingError(f"invalid Bridge candidate work id: {row.get('openalex_id')!r}")
    return token


def _candidate_openalex_id(row: Mapping[str, Any], token: str) -> str:
    raw = str(row.get("openalex_id") or "").strip()
    if raw:
        return raw
    return f"{OPENALEX_WORK_URL_PREFIX}{token}"


def _candidate_work_id_int(row: Mapping[str, Any]) -> int:
    value = row.get("work_id_int")
    if isinstance(value, bool) or value is None:
        raise MLBridgeScorerRolloutServingError("Bridge candidate missing integer work_id_int")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise MLBridgeScorerRolloutServingError("Bridge candidate missing integer work_id_int") from exc


def _bridge_score(row: Mapping[str, Any]) -> float:
    value = _as_float(row.get("bridge_score"))
    if value is None:
        raise MLBridgeScorerRolloutServingError("Bridge candidate missing bridge_score")
    return value


def _rank_bridge_candidates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    ml_prob_by_token: Mapping[str, float],
    alpha: float = PRIMARY_ALPHA,
) -> list[dict[str, Any]]:
    if not candidates:
        raise MLBridgeScorerRolloutServingError("Bridge candidate pool is empty")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in candidates:
        token = _candidate_token(row)
        if token in seen:
            raise MLBridgeScorerRolloutServingError(f"duplicate Bridge candidate work_id_token {token!r}")
        seen.add(token)
        normalized.append(
            {
                "work_id_int": _candidate_work_id_int(row),
                "work_id_token": token,
                "openalex_id": _candidate_openalex_id(row, token),
                "bridge_score": _bridge_score(row),
                "final_score": _as_float(row.get("final_score")),
                "current_family_rank": row.get("current_family_rank"),
            }
        )

    pool_for_pct = [
        {"work_id_token": row["work_id_token"], "bridge_score": row["bridge_score"]}
        for row in normalized
    ]
    ml_rank_pct_by_token, bridge_rank_pct_by_token = _compute_rank_percentiles_from_pool(
        pool_for_pct,
        ml_prob_by_token=ml_prob_by_token,
    )

    scored: list[dict[str, Any]] = []
    for row in normalized:
        token = row["work_id_token"]
        if token not in ml_prob_by_token:
            raise MLBridgeScorerRolloutServingError(f"missing ML probability for Bridge candidate {token!r}")
        if token not in bridge_rank_pct_by_token:
            raise MLBridgeScorerRolloutServingError(f"missing bridge_score rank percentile for {token!r}")
        ml_prob = float(ml_prob_by_token[token])
        if not math.isfinite(ml_prob):
            raise MLBridgeScorerRolloutServingError(f"invalid ML probability for Bridge candidate {token!r}")
        ml_rank_pct = float(ml_rank_pct_by_token[token])
        bridge_rank_pct = float(bridge_rank_pct_by_token[token])
        hybrid_score = alpha * ml_rank_pct + (1.0 - alpha) * bridge_rank_pct
        scored.append(
            {
                **row,
                "v3_ml_probability": ml_prob,
                "v3_ml_rank_pct": ml_rank_pct,
                "bridge_score_rank_pct": bridge_rank_pct,
                "hybrid_score": hybrid_score,
            }
        )

    ranked = sorted(scored, key=lambda row: (-float(row["hybrid_score"]), str(row["work_id_token"])))
    for rank, row in enumerate(ranked, start=1):
        row["hybrid_rank"] = rank
    return ranked


def rank_bridge_recommendations_with_scorer(
    *,
    database_url: str | None = None,
    repo_root: str | Path | None = None,
    serving_plan_path: str | Path | None = None,
    limit: int = SCORER_SERVED_LIMIT,
) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    plan_path = Path(serving_plan_path) if serving_plan_path is not None else None
    plan_payload, frozen_scorer = _load_serving_plan_and_frozen_scorer(
        root=root,
        serving_plan_path=plan_path,
    )
    db_url = database_url or database_url_from_env()
    candidates = _fetch_bridge_candidates_from_db(db_url, ranking_run_id=PINNED_RANKING_RUN_ID)
    expected_count = int(
        plan_payload.get("pinned_run_context", {}).get("candidate_count") or EXPECTED_CANDIDATE_COUNT
    )
    if len(candidates) != expected_count:
        raise MLBridgeScorerRolloutServingError(
            f"Bridge candidate pool incomplete: {len(candidates)}/{expected_count}"
        )
    pool_for_scoring = [
        {
            "work_id_int": _candidate_work_id_int(row),
            "work_id_token": _candidate_token(row),
            "bridge_score": _bridge_score(row),
        }
        for row in candidates
    ]
    ml_prob_by_token = _score_pool_ml_probabilities(
        pool_for_scoring,
        frozen_scorer=frozen_scorer,
        database_url=db_url,
    )
    ranked_rows = _rank_bridge_candidates(
        candidates,
        ml_prob_by_token=ml_prob_by_token,
        alpha=PRIMARY_ALPHA,
    )
    metadata = {
        "ranking_run_id": PINNED_RANKING_RUN_ID,
        "family": FAMILY,
        "candidate_count": len(candidates),
        "scored_candidate_count": len(ranked_rows),
        "returned_count": min(limit, len(ranked_rows)),
        "limit": limit,
        "primary_alpha": PRIMARY_ALPHA,
        "rank_pct_scope": RANK_PCT_SCOPE,
        "embedding_version": frozen_scorer.get("embedding_version"),
        "scorer_probability_source": "full_pool_frozen_inference_not_oof",
        "writes_performed": False,
    }
    return ranked_rows[:limit], metadata


def map_bridge_scorer_rows_to_paper_ids(
    scored_rows: Sequence[Mapping[str, Any]],
    limit: int = SCORER_SERVED_LIMIT,
) -> list[str]:
    out: list[str] = []
    for row in scored_rows[:limit]:
        paper_id = str(row.get("openalex_id") or "").strip()
        token = str(row.get("work_id_token") or "").strip()
        if not paper_id and token:
            paper_id = f"{OPENALEX_WORK_URL_PREFIX}{token}"
        elif paper_id.startswith("W"):
            paper_id = f"{OPENALEX_WORK_URL_PREFIX}{paper_id}"
        if paper_id:
            out.append(paper_id)
    return out
