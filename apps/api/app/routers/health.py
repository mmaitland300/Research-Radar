from __future__ import annotations

import psycopg
from fastapi import APIRouter, HTTPException

from app.contracts import HealthResponse, ReadinessResponse, utc_now
from app.demo_fixtures import fixture_mode_enabled, fixture_readiness
from app.papers_repo import database_url_from_env

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", timestamp=utc_now())


@router.get("/readyz", response_model=ReadinessResponse)
def readiness() -> ReadinessResponse:
    if fixture_mode_enabled():
        return fixture_readiness()
    try:
        with psycopg.connect(database_url_from_env()) as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database unreachable.") from exc
    return ReadinessResponse(status="ok", database="connected", timestamp=utc_now())
