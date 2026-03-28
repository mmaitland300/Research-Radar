export default function PaperDetailPage({
  params
}: {
  params: { paperId: string };
}) {
  return (
    <main className="page">
      <section className="panel">
        <p className="accent">Paper Detail</p>
        <h1>Per-paper ranking breakdown.</h1>
        <p>
          This page will explain why a paper was surfaced, which clusters it is
          near, and which baseline ranking it outperformed.
        </p>
        <div className="meta">
          <span>Paper ID: {params.paperId}</span>
          <span>Signal breakdown: semantic, velocity, growth, bridge, diversity</span>
        </div>
      </section>

      <section className="split">
        <article className="panel">
          <h2>Why recommended</h2>
          <ul>
            <li>Final score and per-signal contributions</li>
            <li>Nearest cluster labels</li>
            <li>Why it is emerging or bridge-oriented</li>
            <li>Which baseline it beat</li>
          </ul>
        </article>
        <article className="panel">
          <h2>Metadata block</h2>
          <p>
            Abstract, authors, venue, year, linked citations, and ranking-run
            provenance will live here.
          </p>
        </article>
      </section>
    </main>
  );
}
