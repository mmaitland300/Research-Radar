# Week 1 Day 1 - Baseline Freeze Record

Date: `2026-04-25`
Owner: `@mmaitland300`
Status: `complete`

## Environment Pins and Endpoints

- `NEXT_PUBLIC_RANKING_VERSION`: `bridge-v2-nm1-zero-r3-k6-20260424`
- `NEXT_PUBLIC_EMBEDDING_VERSION`: `v1-title-abstract-1536-cleantext-r3`
- Live site URL used for screenshots: `https://radar.mmaitland.dev`
- API base URL used for Day 1 checks: `https://capable-light-production.up.railway.app`
- Intended custom API hostname: `https://api.radar.mmaitland.dev`
- Current DNS status (from terminal on `2026-04-25`): `api.radar.mmaitland.dev` = `NXDOMAIN` (unresolvable)
- Practical Day 1 API check path: use the Railway API URL directly. Custom API DNS can be fixed after the baseline freeze.

## Screenshot Storage Convention

- Store baseline screenshots under:
  - `docs/audit/screenshots/2026-04-25-baseline/`
- Suggested file names:
  - `recommended-emerging.png`
  - `recommended-bridge.png`
  - `recommended-undercited.png`
  - `evaluation-emerging.png`
  - `evaluation-bridge.png`
  - `evaluation-undercited.png`

## Baseline Provenance (Prefilled)

- `corpus_snapshot_version`: `source-snapshot-20260425-044015`
- `embedding_version`: `v1-title-abstract-1536-cleantext-r3`
- `cluster_version`: `kmeans-l2-v0-cleantext-r3-k6`
- `ranking_version`: `bridge-v2-nm1-zero-r3-k6-20260424`
- `ranking_run_id`: `rank-3904fec89d`

## Corpus Counts

- included works: `59`
- excluded works: `517`
- excluded-by-reason:
  - `topic_gate_failed`: `487`
  - `explicit_exclusion_term`: `25`
  - `edge_term_without_core_signal`: `5`

## Day 1 Checklist

- [x] Capture screenshot: `/recommended?family=emerging`
- [x] Capture screenshot: `/recommended?family=bridge`
- [x] Capture screenshot: `/recommended?family=undercited`
- [x] Capture screenshot: `/evaluation?family=emerging`
- [x] Capture screenshot: `/evaluation?family=bridge`
- [x] Capture screenshot: `/evaluation?family=undercited`
- [x] Confirm live site shows expected run provenance (run id/version/snapshot/embedding) on Evaluation
- [x] Confirm `/readyz` returns healthy in current environment via Railway API URL
- [x] Confirm Railway `ranking_runs` row for `rank-3904fec89d` is `succeeded`
- [x] Record exact URLs/timestamps for screenshots below

## Screenshot Register

Use this table to make later comparisons deterministic.

| Surface | URL | Timestamp (UTC) | Notes |
| --- | --- | --- | --- |
| Recommended Emerging | `https://radar.mmaitland.dev/recommended?family=emerging` | `2026-04-25T16:38:19Z` | `docs/audit/screenshots/2026-04-25-baseline/recommended-emerging.png` |
| Recommended Bridge | `https://radar.mmaitland.dev/recommended?family=bridge` | `2026-04-25T16:38:19Z` | `docs/audit/screenshots/2026-04-25-baseline/recommended-bridge.png` |
| Recommended Undercited | `https://radar.mmaitland.dev/recommended?family=undercited` | `2026-04-25T16:38:19Z` | `docs/audit/screenshots/2026-04-25-baseline/recommended-undercited.png` |
| Evaluation Emerging | `https://radar.mmaitland.dev/evaluation?family=emerging` | `2026-04-25T16:38:19Z` | `docs/audit/screenshots/2026-04-25-baseline/evaluation-emerging.png` |
| Evaluation Bridge | `https://radar.mmaitland.dev/evaluation?family=bridge` | `2026-04-25T16:38:19Z` | `docs/audit/screenshots/2026-04-25-baseline/evaluation-bridge.png` |
| Evaluation Undercited | `https://radar.mmaitland.dev/evaluation?family=undercited` | `2026-04-25T16:38:19Z` | `docs/audit/screenshots/2026-04-25-baseline/evaluation-undercited.png` |

## Verification Commands (Optional copy/paste)

```powershell
# set API base for this terminal
$env:API_BASE_URL = "https://capable-light-production.up.railway.app"

# DNS checks first
nslookup radar.mmaitland.dev
nslookup api.radar.mmaitland.dev

# readiness
curl "$env:API_BASE_URL/readyz"

# evaluation families
curl "$env:API_BASE_URL/api/v1/evaluation/compare?family=emerging&limit=12"
curl "$env:API_BASE_URL/api/v1/evaluation/compare?family=bridge&limit=12"
curl "$env:API_BASE_URL/api/v1/evaluation/compare?family=undercited&limit=12"

# Railway ranking run verification
psql -d "$env:RAILWAY_DATABASE_URL" -c "select ranking_run_id, ranking_version, status, corpus_snapshot_version, embedding_version, finished_at from ranking_runs where ranking_run_id = 'rank-3904fec89d';"
```

If `RAILWAY_DATABASE_URL` is unset, set it first (PowerShell):

```powershell
$env:RAILWAY_DATABASE_URL = "<paste railway postgres url>"
```

## Terminal Findings (2026-04-25)

- `nslookup radar.mmaitland.dev` resolved to Vercel.
- `nslookup api.radar.mmaitland.dev` returned `Non-existent domain` (NXDOMAIN).
- `https://capable-light-production.up.railway.app/readyz` returned `{"status":"ok","database":"connected",...}`.
- Evaluation API checks for `emerging`, `bridge`, and `undercited` returned `ranking_run_id=rank-3904fec89d`, `corpus_snapshot_version=source-snapshot-20260425-044015`, and `embedding_version=v1-title-abstract-1536-cleantext-r3`.
- Earlier `psql -d "$env:RAILWAY_DATABASE_URL"` failures to `localhost:5432` indicated `RAILWAY_DATABASE_URL` was not set in that shell.

## Notes

- Follow-up: configure `api.radar.mmaitland.dev` DNS/custom domain if we want a branded API host. Day 1 API evidence uses the working Railway service URL.
- Keep this file immutable once Day 1 is complete. If baseline changes, create a new dated freeze record.
