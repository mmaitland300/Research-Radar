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
  const params = new URLSearchParams({
    limit: "20",
    min_works: "2",
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
        <h1>Track topic growth without losing the paper-level story.</h1>
        <p>
          This view stays inside the curated corpus and asks a simple question:
          which topic labels are showing up more often in recent papers than in
          the earlier slice?
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
          <h2>Topic momentum</h2>
          <p className="muted-inline">
            Since year: <strong>{data.since_year}</strong> | minimum works: <strong>{data.min_works}</strong> | topics returned: <strong>{data.total}</strong>
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
            This is a corpus-scoped topic view, not a global OpenAlex trend chart.
            It is meant to support ranking intuition and future topic-growth
            signals, not replace the paper-level recommendation surfaces.
          </p>
        </article>
        <article className="panel">
          <h2>What is still ahead</h2>
          <p>
            Cluster-level growth, venue/topic composition shifts, and richer
            evaluation against baselines still belong to later milestones.
          </p>
        </article>
      </section>
    </main>
  );
}
