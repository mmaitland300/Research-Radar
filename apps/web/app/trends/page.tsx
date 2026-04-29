import Link from "next/link";

const API_BASE_URL =
  process.env.API_BASE_URL ?? process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

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
    since_year: String(currentYear - 1)
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
  const maxAbsDelta =
    data?.items.reduce((max, item) => Math.max(max, Math.abs(item.delta)), 0) ?? 0;

  return (
    <main className="page">
      <section className="panel page-hero">
        <div className="panel-header">
          <div>
            <p className="eyebrow family-bridge">Trends</p>
            <h1>Topic momentum inside your curated slice only.</h1>
          </div>
          <div className="stamp-row">
            <span className="stamp">Curated corpus trends</span>
            <span className="stamp">Momentum sanity check</span>
          </div>
        </div>
        <p className="hero-lead">
          Counts and deltas are computed over <strong>included works</strong> in Research Radar
          (the same ingestion policy as search and ranking), not OpenAlex-wide. We compare papers
          from the selected &quot;recent&quot; year window against an earlier band to label topics rising,
          steady, or cooling: a sanity check for the corpus, not a field-wide forecast.
        </p>
        <p className="muted-inline">
          Topic labels are imported metadata and can be noisy; use them as coarse navigation hints,
          not authoritative classifications.
        </p>
        {data ? (
          <div className="hero-metrics" aria-label="Trend summary">
            <article className="metric-card">
              <p className="metric-label">Snapshot</p>
              <p className="metric-value metric-value-mono">{data.corpus_snapshot_version}</p>
            </article>
            <article className="metric-card">
              <p className="metric-label">Topics shown</p>
              <p className="metric-value">{data.total}</p>
            </article>
            <article className="metric-card">
              <p className="metric-label">Since year</p>
              <p className="metric-value">{data.since_year}</p>
            </article>
            <article className="metric-card">
              <p className="metric-label">Min works</p>
              <p className="metric-value">{data.min_works}</p>
            </article>
          </div>
        ) : null}
      </section>

      {error ? (
        <section className="panel instrument-panel">
          <h2>Trends unavailable</h2>
          <p>{error}</p>
        </section>
      ) : null}

      {data ? (
        <section className="panel section-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow eyebrow-muted">Momentum table</p>
              <h2>Topic momentum (curated corpus)</h2>
            </div>
            <div className="stamp-row">
              <span className="stamp">Snapshot {data.corpus_snapshot_version}</span>
              <span className="stamp">Since year {data.since_year}</span>
              <span className="stamp">{data.total} topics</span>
            </div>
          </div>
          <p className="muted-inline">
            Recent band uses API <code>since_year</code>, aligned with topic-growth heuristics.
            Minimum works per topic: <strong>{data.min_works}</strong>.
          </p>
          {data.items.length === 0 ? (
            <p>No topic rows matched the current thresholds.</p>
          ) : (
            <ul className="result-list">
              {data.items.map((item) => (
                <li key={item.topic_id} className="result-item result-item-bridge">
                  <div className="result-heading">
                    <h3 className="result-title">{item.topic_name}</h3>
                    <span className={`trend-pill trend-pill-${item.growth_label}`}>
                      {growthLabelText(item.growth_label)}
                    </span>
                  </div>
                  <p className="result-meta">
                    total works: {item.total_works} | recent: {item.recent_works} | prior:{" "}
                    {item.prior_works} | delta: {item.delta}
                  </p>
                  <div className="trend-meter" aria-hidden="true">
                    <div
                      className={`trend-meter-fill trend-meter-fill-${item.growth_label}`}
                      style={{
                        width:
                          maxAbsDelta > 0
                            ? `${Math.max(10, Math.round((Math.abs(item.delta) / maxAbsDelta) * 100))}%`
                            : "10%"
                      }}
                    />
                  </div>
                  <p className="result-reason">
                    Use this as a corpus-level readout for topic growth signals, not as a claim
                    about the full field.
                  </p>
                </li>
              ))}
            </ul>
          )}
        </section>
      ) : null}

      <section className="split">
        <article className="panel instrument-panel">
          <h2>What this means</h2>
          <p>
            If a label rises here, it means your current venue mix and inclusion rules are
            producing more papers tagged with that OpenAlex topic in the recent window: useful
            for interpreting topic_growth-style signals, not for claiming the whole field moved.
          </p>
        </article>
        <article className="panel instrument-panel">
          <h2>What is still ahead</h2>
          <p>
            Corpus-snapshot-specific trends, cluster dynamics, and labeled evaluation (see{" "}
            <Link href="/evaluation">Evaluation</Link>) sit outside this v0 view.
          </p>
        </article>
      </section>
    </main>
  );
}
