const metrics = [
  { label: "Benchmark checks", value: "3" },
  { label: "Target benchmark size", value: "100-200" },
  { label: "Primary cutoffs", value: "P@10 / P@20" }
];

export default function EvaluationPage() {
  return (
    <main className="page">
      <section className="panel">
        <p className="accent">Evaluation</p>
        <h1>The MVP is incomplete without evidence.</h1>
        <p>
          Evaluation is a product page, not an internal notebook. The first
          release should compare against citation-only baselines and include a
          temporal backtest that freezes the world at time T.
        </p>
      </section>

      <section className="grid">
        {metrics.map((metric) => (
          <article className="card" key={metric.label}>
            <h2>{metric.label}</h2>
            <p className="metric">{metric.value}</p>
          </article>
        ))}
      </section>

      <section className="split">
        <article className="panel">
          <h2>Required checks</h2>
          <ul>
            <li>Hand-reviewed relevance benchmark</li>
            <li>Novelty and diversity versus citation-only baselines</li>
            <li>Freeze-at-T temporal backtest</li>
          </ul>
        </article>
        <article className="panel">
          <h2>Failure modes to report</h2>
          <ul>
            <li>Cluster collapse around trendy topics</li>
            <li>Venue bias from core-source allowlists</li>
            <li>Bridge inflation from noisy edge-slice papers</li>
          </ul>
        </article>
      </section>
    </main>
  );
}
