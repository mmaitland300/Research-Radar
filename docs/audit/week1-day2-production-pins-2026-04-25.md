# Week 1 Day 2 - Production pins + deploy verification

Date: `2026-04-25`
Owner: `@mmaitland300`
Status: `complete`

Baseline reference: `docs/audit/week1-day1-baseline-freeze-2026-04-25.md`

## Objective

Confirm production hosts inject `NEXT_PUBLIC_EMBEDDING_VERSION` and `NEXT_PUBLIC_RANKING_VERSION` that match the frozen baseline, so the live product is not relying on implicit defaults.

## Evidence

Railway project **`research-radar`** → **Environment Variables** (Project scope, All Environments), captured in:

- `docs/audit/screenshots/2026-04-25-baseline/railway-project-next-public-pins.png`

Recorded values (verify visually in screenshot; ranking label uses **`nm1`**, digit one, not `nmi`):

| Variable | Value (must match Day 1 baseline) |
| --- | --- |
| `NEXT_PUBLIC_EMBEDDING_VERSION` | `v1-title-abstract-1536-cleantext-r3` |
| `NEXT_PUBLIC_RANKING_VERSION` | `bridge-v2-nm1-zero-r3-k6-20260424` |

## Cross-checks already satisfied under Day 1

- API and UI surfaced `ranking_run_id=rank-3904fec89d` and matching snapshot / embedding provenance for evaluation families.
- `/readyz` healthy against Railway service URL documented in the Day 1 freeze.

## Notes

- Custom API hostname (`api.radar.mmaitland.dev`) remains optional; Day 2 closure does not depend on it.
- If Railway-only variables drive the browser build, ensure the **frontend deploy** that users hit (e.g. Vercel) either reads the same pins or is documented separately; this record covers Railway project env as captured.
