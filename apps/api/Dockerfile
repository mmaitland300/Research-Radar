# Build from repository root so pipeline + audit artifacts are available at runtime:
#   docker build -f apps/api/Dockerfile .
FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV RESEARCH_RADAR_REPO_ROOT=/app

# Keep services/pipeline/pipeline/repo_paths.py -> default_repo_root() == /app
COPY services/pipeline ./services/pipeline
COPY apps/api ./apps/api
COPY docs/audit/ml-offline-audit-embedding-scorer-v2.json ./docs/audit/ml-offline-audit-embedding-scorer-v2.json
COPY docs/audit/ml-bridge-rank-pct-hybrid-serving-plan-v1.json ./docs/audit/ml-bridge-rank-pct-hybrid-serving-plan-v1.json
COPY docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json ./docs/audit/ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json
COPY docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json ./docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir ./services/pipeline \
    && pip install --no-cache-dir ./apps/api \
    && chmod +x ./apps/api/docker-entrypoint.sh

ENTRYPOINT ["./apps/api/docker-entrypoint.sh"]
