# Contributing

Research Radar is open to focused contributions that improve reproducibility,
API correctness, ranking transparency, setup clarity, and documentation
accuracy. It is still a prototype, so small, well-scoped changes are more useful
than broad rewrites.

## Good First Areas

- Reproducible API bugs, especially in `apps/api`
- Fixture demo issues that block `npm run demo:local`
- Ranking, trends, evaluation, or similar-papers bugs with a pinned request
- Tests that lock down an existing route or data-contract behavior
- Documentation corrections that clarify setup, limits, or current evidence
- Small UI fixes that make the existing product surfaces easier to inspect

## Out Of Scope For Drive-By PRs

- Claims that Research Radar is a validated recommender without new evidence
- Large architecture rewrites without an issue first
- Tests that require live OpenAlex/OpenAI calls
- Committed secrets, database dumps, or private provider payloads
- Ranking or evaluation copy that removes documented limitations

## Local Setup

For a quick no-key walkthrough:

```bash
pip install -e ./apps/api
npm install
npm run demo:local
```

Fixture mode runs the real API and web app against checked-in fixture data. It
does not require Postgres, pgvector, OpenAlex, or OpenAI credentials.

For the full local path, follow the README's "Full Postgres Run Locally"
section and configure the required environment variables.

## Checks

Run the relevant checks before opening a pull request:

```bash
npm run validate:web
npm run validate:py
```

For a full pass:

```bash
npm run validate
```

If you only changed Markdown or issue templates, at minimum check formatting and
YAML validity before opening the PR.

## Ranking And Evaluation Changes

Ranking changes should preserve provenance. When a behavior depends on a
specific run or artifact, include the relevant identifiers where possible:

- `ranking_run_id`
- `ranking_version`
- `corpus_snapshot_version`
- `embedding_version`
- `cluster_version`

Evaluation language should stay precise. Proxy metrics are useful for iteration
but should not be described as human-labeled relevance validation.

## Pull Request Style

- Keep PRs small and focused.
- Explain what changed and why.
- Include commands you ran.
- Add or update tests when behavior changes.
- Redact secrets from screenshots, logs, and fixtures.

For security-sensitive reports, see `SECURITY.md` instead of opening a detailed
public issue.
