FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml ./
COPY app ./app
COPY docker-entrypoint.sh /docker-entrypoint.sh

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir . \
    && chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
