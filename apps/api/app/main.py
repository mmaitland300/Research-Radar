from __future__ import annotations

from fastapi import FastAPI

from app.routers import (
    clusters,
    evaluation,
    health,
    meta,
    papers,
    recommendations,
    search,
    trends,
)

app = FastAPI(
    title="Research Radar API",
    version="0.1.0",
    description="API surface for ranking, explainability, and evaluation in the Research Radar project.",
)

app.include_router(health.router)
app.include_router(meta.router)
app.include_router(recommendations.router)
app.include_router(evaluation.router)
app.include_router(trends.router)
app.include_router(clusters.router)
app.include_router(papers.router)
app.include_router(search.router)
