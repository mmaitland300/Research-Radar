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
- Bridge remains experimental; treat `/recommended?family=bridge` as diagnostics, not a validated recommender until weighting ships
- General query-semantic search is not yet shipped; some pinned **emerging** runs may use embedding **slice-fit** as one bounded feature when labeled in the UI

## Best proof links

- Ranked recommendation API tests: [apps/api/tests/test_recommendations_ranked.py](https://github.com/mmaitland300/Research-Radar/blob/main/apps/api/tests/test_recommendations_ranked.py)
- Evaluation API tests: [apps/api/tests/test_evaluation_compare.py](https://github.com/mmaitland300/Research-Radar/blob/main/apps/api/tests/test_evaluation_compare.py)
- Ranked explanation surface: [apps/web/app/recommended/page.tsx](https://github.com/mmaitland300/Research-Radar/blob/main/apps/web/app/recommended/page.tsx)
- Live routes:
  - [Recommended (Emerging)](https://radar.mmaitland.dev/recommended?family=emerging)
  - [Paper detail example](https://radar.mmaitland.dev/papers/https%3A%2F%2Fopenalex.org%2FW3093121331)
  - [Trends](https://radar.mmaitland.dev/trends)
  - [Evaluation (Emerging)](https://radar.mmaitland.dev/evaluation?family=emerging)
