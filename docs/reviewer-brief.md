# Research Radar reviewer brief

## Current deployed slice

- Search: lexical title/abstract retrieval over included works
- Recommended: materialized ranking runs for emerging, bridge, and undercited families
- Paper detail: metadata, topics, ranking placement, and similar papers when `NEXT_PUBLIC_EMBEDDING_VERSION` is configured
- Trends: corpus-scoped topic momentum
- Evaluation: ranked output vs citation/date baselines (proxy-only)

## Current limits

- Corpus is intentionally narrow and currently wired to TISMIR + JAES
- Evaluation is proxy/distributional and not a human-labeled relevance benchmark
- Bridge remains experimental and explicitly labeled as such
- Query-semantic search is not yet shipped

## Best proof links

- Ranked recommendation API tests: [apps/api/tests/test_recommendations_ranked.py](https://github.com/mmaitland300/Research-Radar/blob/main/apps/api/tests/test_recommendations_ranked.py)
- Evaluation API tests: [apps/api/tests/test_evaluation_compare.py](https://github.com/mmaitland300/Research-Radar/blob/main/apps/api/tests/test_evaluation_compare.py)
- Ranked explanation surface: [apps/web/app/recommended/page.tsx](https://github.com/mmaitland300/Research-Radar/blob/main/apps/web/app/recommended/page.tsx)
- Live routes:
  - [Recommended (Emerging)](https://radar.mmaitland.dev/recommended?family=emerging)
  - [Paper detail example](https://radar.mmaitland.dev/papers/https%3A%2F%2Fopenalex.org%2FW3093121331)
  - [Trends](https://radar.mmaitland.dev/trends)
  - [Evaluation (Emerging)](https://radar.mmaitland.dev/evaluation?family=emerging)
