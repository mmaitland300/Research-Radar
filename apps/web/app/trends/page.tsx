import Link from "next/link";

const API_BASE_URL = process.env.API_BASE_URL ?? process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

type TopicTrendItem = {
  topic_id: number;
  topic_name: string;
  total_works: number;
  recent_works: number;
  prior_works: number;
  delta: number;
  growth_label: string;
};

type TopicTrendsResponse = {
  corpus_snapshot_version: string;
  since_year: number;
  min_works: number;
  total: number;
  items: TopicTrendItem[];
  generated_at: string;
};

async function fetchTopicTrends(): Promise<{
  data: TopicTrendsResponse | null;
  error: string | null;
}> {
  const currentYear = new Date().getUTCFullYear();
  const params = new URLSearchParams({
    limit: "24",
    min_works: "2",
    since_year: String(currentYear - 1),
  });

  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/trends/topics?${params.toString()}`, {
      cache: "no-store"
    });

    if (!response.ok) {
      return {
        data: null,
        error: `API returned ${response.status} for /api/v1/trends/topics`
      };
    }

    const data = (await response.json()) as TopicTrendsResponse;
    return { data, error: null };
  } catch (error) {
    return {
      data: null,
      error: error instanceof Error ? error.message : "Unknown error"
    };
  }
}

function growthLabelText(label: string): string {
  if (label === "rising") {
    return "rising";
  }
  if (label === "cooling") {
    return "cooling";
  }
  return "steady";
}

export default async function TrendsPage() {
  const { data, error } = await fetchTopicTrends();

  return (
    <main className="page">
      <section className="panel">
        <p className="accent-2">Trends</p>
        <h1>Topic momentum inside your curated slice only.</h1>
        <p>
          Counts and deltas are computed over <strong>included works</strong> in Research Radar
          (the same ingestion policy as search and ranking), not OpenAlex-wide. We compare papers
          from the selected &quot;recent&quot; year window against an earlier band to label topics
          rising, steady, or cooling — a sanity check for the corpus, not a field-wide forecast.
        </p>
      </section>

      {error ? (
        <section className="panel">
          <h2>Trends unavailable</h2>
          <p>{error}</p>
        </section>
      ) : null}

      {data ? (
        <section className="panel">
          <h2>Topic momentum (curated corpus)</h2>
          <p className="muted-inline">
            Snapshot <code>{data.corpus_snapshot_version}</code> | recent band starts at year{" "}
            <strong>{data.since_year}</strong> (API <code>since_year</code>, aligned with
            topic-growth heuristics) | min works per topic: <strong>{data.min_works}</strong> |
            topics shown: <strong>{data.total}</strong>
          </p>
          {data.items.length === 0 ? (
            <p>No topic rows matched the current thresholds.</p>
          ) : (
            <ul className="result-list">
              {data.items.map((item) => (
                <li key={item.topic_id} className="result-item">
                  <h3 className="result-title">{item.topic_name}</h3>
                  <p className="result-meta">
                    total works: {item.total_works} | recent: {item.recent_works} | prior: {item.prior_works} | delta: {item.delta} | label: {growthLabelText(item.growth_label)}
                  </p>
                </li>
              ))}
            </ul>
          )}
        </section>
      ) : null}

      <section className="split">
        <article className="panel">
          <h2>What this means</h2>
          <p>
            If a label rises here, it means your current venue mix and inclusion
            rules are producing more papers tagged with that OpenAlex topic in the
            recent window — useful for interpreting topic_growth-style signals, not
            for claiming the whole field moved.
          </p>
        </article>
        <article className="panel">
          <h2>What is still ahead</h2>
          <p>
            Corpus-snapshot–specific trends, cluster dynamics, and labeled
            evaluation (see <Link href="/evaluation">Evaluation</Link>) sit outside
            this v0 view.
          </p>
        </article>
      </section>
    </main>
  );
}
