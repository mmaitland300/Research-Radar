# Research Radar reviewer brief

## Current deployed slice

- Search: lexical title/abstract retrieval over included works
- Recommended: materialized ranking runs for emerging and undercited families; **bridge** route is a **preview/diagnostics** surface (bridge signal measured; not weighted into `final_score` in the current public run)
- Paper detail: metadata, topics, ranking placement, and similar papers when `NEXT_PUBLIC_EMBEDDING_VERSION` is configured
- Trends: corpus-scoped topic momentum
- Evaluation: ranked output vs citation/date baselines (proxy-only)

## Current limits

- Corpus is intentionally narrow and currently wired to TISMIR + JAES
- Evaluation is proxy/distributional and not a human-labeled relevance benchmark
- Bridge remains experimental; treat `/recommended?family=bridge` as diagnostic evidence, not a validated bridge recommender. Default readiness and stronger product claims require further evidence—not bridge weighting alone (`ready_for_default=false` in audit artifacts).
- General query-semantic search is not yet shipped; some pinned **emerging** runs may use embedding **slice-fit** as one bounded feature when labeled in the UI

## Recommended bridge guardrails

The default bridge route is the full bridge preview:

- Local default bridge preview: `http://localhost:3000/recommended?family=bridge`
- Public/default behavior must keep eligible-only bridge controls hidden.
- Direct visits to `http://localhost:3000/recommended?family=bridge&bridge_eligible_only=true` are guarded unless the experimental flag is enabled; the page shows the full bridge preview with a disabled-view notice.

The experimental eligible-only bridge review UI is controlled by:

- `NEXT_PUBLIC_ENABLE_EXPERIMENTAL_BRIDGE_VIEW`
- Default: `false`
- Normal/public runs should leave it disabled.

To inspect the current experimental bridge review view locally, set `NEXT_PUBLIC_ENABLE_EXPERIMENTAL_BRIDGE_VIEW=true`, restart the web app, and deliberately pin the objective run:

`http://localhost:3000/recommended?family=bridge&ranking_run_id=rank-60910a47b4&bridge_eligible_only=true`

When this view is exposed, it is still experimental and must be read with the page copy: "Experimental bridge review view; not validated or default." and "Single-reviewer, top-20, offline audit evidence only."

## Ranking run pinning

`NEXT_PUBLIC_RANKING_VERSION` filters the Recommended page to a ranking version label when it is set. A `ranking_run_id` URL parameter pins a specific materialized run for inspection and should be used for reviewer evidence checks. If neither is set, the app resolves the latest succeeded run, which is convenient locally but should not be treated as a stable review reference.

The current evidence-backed bridge objective experiment uses:

- Baseline bridge run: `rank-ee2ba6c816`
- Objective experiment run: `rank-60910a47b4`
- Eligibility mode: `top50_cross040_exclude_persistent_shared_v1`
- Evidence index: `docs/audit/bridge-evidence-summary.md`

Eligible-only bridge remains experimental because the evidence is single-reviewer, top-20, offline audit material only. It is not validation, not a superiority claim, and not default readiness.

## Best proof links

- Ranked recommendation API tests: [apps/api/tests/test_recommendations_ranked.py](https://github.com/mmaitland300/Research-Radar/blob/main/apps/api/tests/test_recommendations_ranked.py)
- Evaluation API tests: [apps/api/tests/test_evaluation_compare.py](https://github.com/mmaitland300/Research-Radar/blob/main/apps/api/tests/test_evaluation_compare.py)
- Ranked explanation surface: [apps/web/app/recommended/page.tsx](https://github.com/mmaitland300/Research-Radar/blob/main/apps/web/app/recommended/page.tsx)
- Live routes:
  - [Recommended (Emerging)](https://radar.mmaitland.dev/recommended?family=emerging)
  - [Paper detail example](https://radar.mmaitland.dev/papers/https%3A%2F%2Fopenalex.org%2FW3093121331)
  - [Trends](https://radar.mmaitland.dev/trends)
  - [Evaluation (Emerging)](https://radar.mmaitland.dev/evaluation?family=emerging)
