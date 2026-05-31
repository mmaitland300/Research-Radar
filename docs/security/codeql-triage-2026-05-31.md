# CodeQL Triage - 2026-05-31

Snapshot source: GitHub code scanning API for open alerts on `main`.

## Alert Snapshot

| Rule | Count | Initial disposition |
| --- | ---: | --- |
| `py/sql-injection` | 2 | Real fix |
| `py/incomplete-url-substring-sanitization` | 1 | Test-only cleanup |
| `py/weak-sensitive-data-hashing` | 3 | Non-security checksum false-positive review |
| `py/clear-text-logging-sensitive-data` | 45 | Audit-artifact false-positive review |
| `py/clear-text-storage-sensitive-data` | 38 | Audit-artifact false-positive review |

## Fixes In This Pass

- `apps/api/app/evaluation_repo.py`: removed dynamic `ORDER BY {order_clause}` SQL construction. Baseline ordering now uses a static SQL shape with a parameterized ordering mode.
- `services/pipeline/tests/test_ranking_run.py`: replaced substring host checking with `urllib.parse.urlparse`.

## Remaining Triage Bucket

The weak-hash alerts are SHA-256 artifact fingerprints, not password or secret hashing. The clear-text logging/storage alerts point at CLI status prints and local audit artifact writes. The reviewed examples write paths, counts, run IDs, `recommended_next_stage`, redacted database targets, or generated audit payloads. They should not be described as a clean security posture until each alert is either dismissed in GitHub with this rationale or replaced with narrower safe-output helpers.

Next review action: confirm the next CodeQL run closes the SQL and URL alerts, then process the remaining weak-hash and clear-text alerts in batches with explicit GitHub dismissal comments or targeted code changes where a payload can include unredacted connection error text.
