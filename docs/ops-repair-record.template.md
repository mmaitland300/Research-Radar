# Ops: text repair / re-embed record (copy to a local file)

Copy this file to **`ops-repair-last-run.md`** in the repo root and fill after each production cycle.  
In your clone, add `ops-repair-last-run.md` to **`.git/info/exclude`** so the filled file never gets committed.

| Field | Value |
|-------|--------|
| Date (UTC or local + TZ) | |
| `corpus_snapshot_version` | |
| `repair-works-text` dry-run `rows_changed` | |
| `repair-works-text` apply `rows_changed` (if run) | |
| `embedding_version` (`EMBED_VER`) | |
| `cluster_version` (`CLUSTER_VER`) | |
| `ranking_version` (if any) | |
| `ranking_run_id` (if any) | |
| Notes (API pin, product env, etc.) | |

Paste the exact stderr line from `repair-works-text` below:

```
(paste here)
```
