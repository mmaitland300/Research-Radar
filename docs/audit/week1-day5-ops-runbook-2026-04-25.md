# Week 1 Day 5 - Ops runbook (practical)

Date: `2026-04-25`  
Owner: `@mmaitland300`  
Status: `complete`

Purpose: recover pipeline state quickly and safely when ingest/embedding/clustering/ranking drift.

## 0) Preconditions

- Run from `services/pipeline`.
- Set `DATABASE_URL` for the target environment.
- Use explicit labels for `SNAPSHOT`, `EMBEDDING_VERSION`, `CLUSTER_VERSION`, and `RANKING_VERSION`.

## 1) Ingest stuck/running repair

1. Inspect current ingest state and recent logs.
2. If text quality fixes are needed, run:

```bash
python -m pipeline.cli repair-works-text --corpus-snapshot-version <SNAPSHOT> --dry-run
```

3. If `rows_changed > 0`, run the same command without `--dry-run`, then continue with a **new** embedding label (do not reuse old vectors after text mutation).
4. Re-run ingest/ranking pipeline steps only after the snapshot state is clear.

## 2) Embedding coverage check

Run:

```bash
python -m pipeline.cli embedding-coverage --embedding-version <EMBEDDING_VERSION> --corpus-snapshot-version <SNAPSHOT>
```

Use strict mode before promotion:

```bash
python -m pipeline.cli embedding-coverage --embedding-version <EMBEDDING_VERSION> --corpus-snapshot-version <SNAPSHOT> --fail-on-gaps
```

Expected promotion posture: `missing_embedding=0` for the intended scope.

## 3) Cluster coverage check

After embeddings are complete:

```bash
python -m pipeline.cli cluster-works --embedding-version <EMBEDDING_VERSION> --cluster-version <CLUSTER_VERSION> --corpus-snapshot-version <SNAPSHOT> --cluster-count <K>
```

Then verify cluster coverage:

```bash
python -m pipeline.cli embedding-coverage --embedding-version <EMBEDDING_VERSION> --cluster-version <CLUSTER_VERSION> --corpus-snapshot-version <SNAPSHOT> --fail-on-gaps
```

Expected promotion posture: `missing_cluster_assignment=0`.

## 4) Ranking run status check

Run materialization:

```bash
python -m pipeline.cli ranking-run --ranking-version <RANKING_VERSION> --corpus-snapshot-version <SNAPSHOT> --embedding-version <EMBEDDING_VERSION> --cluster-version <CLUSTER_VERSION> --bridge-weight-for-family-bridge 0
```

Verify in DB:

```sql
select ranking_run_id, ranking_version, status, corpus_snapshot_version, embedding_version, finished_at
from ranking_runs
where ranking_version = '<RANKING_VERSION>'
order by finished_at desc nulls last, started_at desc
limit 5;
```

Require `status='succeeded'` before any pinning/deploy decisions.

## 5) What not to do

- Do **not** reuse `EMBEDDING_VERSION` labels after title/abstract text changes.
- Do **not** ship positive bridge weight without evaluation evidence and quality review.
- Do **not** treat semantic fields as active ordering logic unless run config/explanations say semantic is used.

## 6) Hand-off checklist

- Snapshot label recorded.
- Embedding coverage result recorded.
- Cluster coverage result recorded.
- Latest succeeded `ranking_run_id` recorded.
- Any warnings/known gaps recorded.

## 7) Day 6 decision note (post-Week 1)

Current recommendation: `59` included works is enough for smoke/demo labels, but not enough for strong ML-quality claims. Expand corpus before serious benchmark claims.
