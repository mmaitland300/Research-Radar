export default function TrendsPage() {
  return (
    <main className="page">
      <section className="panel">
        <p className="accent-2">Trends</p>
        <h1>Track cluster growth without losing the paper-level story.</h1>
        <p>
          Trends should answer which topics are rising, which clusters are
          accelerating, and how venue emphasis shifts over time inside the
          curated corpus.
        </p>
      </section>

      <section className="split">
        <article className="panel">
          <h2>Views planned for V1</h2>
          <ul>
            <li>Rising topics over rolling time windows</li>
            <li>Fast-growing clusters by publication and citation delta</li>
            <li>Venue/topic composition shifts</li>
          </ul>
        </article>
        <article className="panel">
          <h2>Out of scope</h2>
          <p>
            Graph exploration is intentionally postponed until ranking quality
            and explainability are strong enough to justify it.
          </p>
        </article>
      </section>
    </main>
  );
}
