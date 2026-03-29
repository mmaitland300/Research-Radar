import Link from "next/link";

const FAMILIES = ["emerging", "bridge", "undercited"] as const;
type Family = (typeof FAMILIES)[number];

const FAMILY_LABEL: Record<Family, string> = {
  emerging: "Emerging",
  bridge: "Bridge",
  undercited: "Under-cited"
};

type RankedSignals = {
  semantic: number | null;
  citation_velocity: number | null;
  topic_growth: number | null;
  bridge: number | null;
  diversity_penalty: number | null;
};

type RankedItem = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  topics: string[];
  signals: RankedSignals;
  final_score: number;
  reason_short: string;
};

type RankedResponse = {
  ranking_run_id: string;
  ranking_version: string;
  corpus_snapshot_version: string;
  family: string;
  total: number;
  items: RankedItem[];
};

const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

/** Optional: pin a pipeline run label, e.g. v0-heuristic-no-embeddings-step3 */
const RANKING_VERSION =
  process.env.NEXT_PUBLIC_RANKING_VERSION?.trim() || undefined;

function parseFamily(raw: string | string[] | undefined): Family {
  const v = Array.isArray(raw) ? raw[0] : raw;
  if (v && (FAMILIES as readonly string[]).includes(v)) {
    return v as Family;
  }
  return "emerging";
}

function formatSignals(signals: RankedSignals): string {
  const entries: [string, number][] = [];
  if (signals.semantic != null) entries.push(["semantic", signals.semantic]);
  if (signals.citation_velocity != null) {
    entries.push(["citation_velocity", signals.citation_velocity]);
  }
  if (signals.topic_growth != null) entries.push(["topic_growth", signals.topic_growth]);
  if (signals.bridge != null) entries.push(["bridge", signals.bridge]);
  if (signals.diversity_penalty != null) {
    entries.push(["diversity_penalty", signals.diversity_penalty]);
  }
  if (entries.length === 0) return "—";
  return entries.map(([k, v]) => `${k}=${Number(v).toFixed(4)}`).join(", ");
}

async function fetchRanked(family: Family): Promise<{
  data: RankedResponse | null;
  error: string | null;
  status: number | null;
}> {
  const params = new URLSearchParams({
    family,
    limit: "15"
  });
  if (RANKING_VERSION) params.set("ranking_version", RANKING_VERSION);

  try {
    const response = await fetch(
      `${API_BASE_URL}/api/v1/recommendations/ranked?${params.toString()}`,
      { cache: "no-store" }
    );
    if (response.status === 404) {
      return {
        data: null,
        error:
          "No succeeded ranking run found. Run the pipeline ranking job against your DB, or set NEXT_PUBLIC_RANKING_VERSION to match an existing run.",
        status: 404
      };
    }
    if (!response.ok) {
      return {
        data: null,
        error: `API responded with ${response.status}.`,
        status: response.status
      };
    }
    const data = (await response.json()) as RankedResponse;
    return { data, error: null, status: 200 };
  } catch {
    return {
      data: null,
      error: "Could not reach the API. Start apps/api and Postgres, then refresh.",
      status: null
    };
  }
}

type PageProps = {
  searchParams: Record<string, string | string[] | undefined>;
};

export default async function RecommendedPage({ searchParams }: PageProps) {
  const family = parseFamily(searchParams.family);
  const { data, error, status } = await fetchRanked(family);

  return (
    <main className="page">
      <section className="panel">
        <p className="accent">Recommended</p>
        <h1>Ranked recommendations</h1>
        <p>
          Papers come from a <strong>materialized ranking run</strong>{" "}
          (<code>paper_scores</code> per family). Semantic and bridge components stay empty
          until embeddings land; citation and topic heuristics still produce an ordered list
          and a short reason per row.
        </p>
        <nav className="tabs" aria-label="Recommendation family">
          {FAMILIES.map((f) => (
            <Link
              key={f}
              href={`/recommended?family=${f}`}
              aria-current={f === family ? "page" : undefined}
            >
              {FAMILY_LABEL[f]}
            </Link>
          ))}
        </nav>
        {data ? (
          <p className="muted-inline">
            <strong>{data.ranking_version}</strong> · run{" "}
            <code>{data.ranking_run_id}</code> · snapshot{" "}
            <code>{data.corpus_snapshot_version}</code> · {data.total} paper
            {data.total === 1 ? "" : "s"}
          </p>
        ) : null}
        {RANKING_VERSION ? (
          <p className="muted-inline">
            Web is filtering runs with{" "}
            <code>NEXT_PUBLIC_RANKING_VERSION={RANKING_VERSION}</code>.
          </p>
        ) : (
          <p className="muted-inline">
            Using the latest succeeded run for the corpus snapshot (set{" "}
            <code>NEXT_PUBLIC_RANKING_VERSION</code> to pin a label).
          </p>
        )}
      </section>

      {error ? (
        <section className="panel">
          <p>{error}</p>
          {status === 404 ? (
            <p className="muted-inline">
              Example:{" "}
              <code>
                NEXT_PUBLIC_RANKING_VERSION=v0-heuristic-no-embeddings-step3
              </code>
            </p>
          ) : null}
        </section>
      ) : null}

      {data && !error ? (
        <section className="panel">
          <h2>{FAMILY_LABEL[family]} · live results</h2>
          <p className="muted-inline">
            Family: <strong>{data.family}</strong> · final_score descending
          </p>
          {data.items.length === 0 ? (
            <p>No rows for this family in the selected run.</p>
          ) : (
            <ul className="result-list">
              {data.items.map((item) => (
                <li key={item.paper_id} className="result-item">
                  <p className="result-title">
                    <Link href={`/papers/${encodeURIComponent(item.paper_id)}`}>
                      {item.title}
                    </Link>
                  </p>
                  <p className="result-meta">
                    {item.year} | cites: {item.citation_count} |{" "}
                    {item.source_slug ?? "unknown venue"} | score:{" "}
                    {item.final_score.toFixed(4)}
                  </p>
                  {item.topics.length > 0 ? (
                    <div className="chip-row" aria-label="Top topics">
                      {item.topics.map((t) => (
                        <span key={t} className="chip">
                          {t}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  <p className="result-reason">{item.reason_short}</p>
                  <p className="result-breakdown">
                    Signals: {formatSignals(item.signals)}
                  </p>
                </li>
              ))}
            </ul>
          )}
        </section>
      ) : null}

      <section className="grid">
        <article className="card">
          <h2>Roadmap: embeddings</h2>
          <p>
            ML milestone 1 fills <code>semantic_score</code> and retrieval; bridge-style
            scores follow once clusters are available.
          </p>
        </article>
        <article className="card">
          <h2>Heuristic baseline</h2>
          <p>
            The rule-only undercited list is still available at{" "}
            <code>/api/v1/recommendations/undercited</code> for comparisons and eval baselines.
          </p>
        </article>
      </section>
    </main>
  );
}
