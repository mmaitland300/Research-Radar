# Security Policy

Research Radar is a public prototype, not a production service with a formal
security response SLA. Security reports are still welcome, especially where
they involve secrets, provider credentials, database access, or exposed ranking
data.

## Supported Scope

Security review currently covers the active `main` branch and the deployed
prototype surface when it matches public code.

In scope:

- FastAPI routes under `apps/api`
- Next.js pages and API-facing client code under `apps/web`
- Pipeline scripts that handle OpenAlex, OpenAI-compatible embedding providers,
  PostgreSQL, pgvector, ranking runs, or evaluation artifacts
- Docker/local setup that could expose secrets or database credentials
- Fixture demo mode if it leaks, stores, or requests data it should not

Out of scope:

- Issues that require access to private infrastructure or private datasets
- Provider availability, quota, billing, or model-quality behavior outside this
  repository's code
- Non-sensitive dependency scanner noise without a practical exploit path

## Secrets And Data

Do not commit or paste:

- `.env` files
- `DATABASE_URL` values or `PGPASSWORD`
- OpenAI, OpenAlex, or other provider API keys
- Private database dumps
- Raw provider payloads that include non-public or licensed data
- Ranking/evaluation artifacts that contain private notes or unreleased data

The no-key demo path uses checked-in fixture data and should not require
Postgres, OpenAlex, or OpenAI credentials.

## Reporting

For sensitive reports, use GitHub private vulnerability reporting for this
repository if it is available. If private reporting is not available, contact
the maintainer through the public portfolio contact path and avoid posting
exploit details, secrets, or live credentials in a public issue.

For non-sensitive bugs, use the issue templates and include a minimal
reproduction.

Helpful details:

- Affected route, command, script, or file
- Whether the issue reproduces in fixture mode or requires the full Postgres path
- Commit SHA or branch
- Redacted logs or stack traces
- Expected impact and why the behavior is unsafe

## Maintainer Response

This is a solo-maintained portfolio project, so response time is best effort.
Confirmed security fixes will be kept narrow, documented in the pull request,
and merged before broader refactors when possible.
