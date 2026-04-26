const cards = [
  {
    title: "Emerging Papers",
    body:
      "Rank papers by citation velocity and local topic growth, with semantic fields shown only when a selected run computes and uses them."
  },
  {
    title: "Bridge preview",
    body:
      "Inspect cross-cluster candidates with measured bridge signal. Weighting into final_score is gated until the evaluation story matches the label."
  },
  {
    title: "Explainable Ranking",
    body:
      "Expose score contributions, cluster context, and baseline comparisons so each recommendation feels engineered."
  },
  {
    title: "Evaluation First",
    body:
      "Ship proxy evaluation and baseline comparison with the MVP so ranking behavior can be inspected before stronger quality claims are made."
  }
];

const signalCards = [
  {
    tone: "emerging",
    label: "Emerging",
    value: "Momentum first",
    body: "Find papers where topic growth and citation velocity are rising inside your curated slice."
  },
  {
    tone: "bridge",
    label: "Bridge",
    value: "Cross-cluster links",
    body:
      "Inspect candidate works that may connect nearby but distinct research neighborhoods, with bridge signal kept diagnostic until weighting and evaluation justify stronger claims."
  },
  {
    tone: "undercited",
    label: "Undercited",
    value: "Early signal",
    body: "Watch low-citation candidates that look stronger than their current attention suggests."
  }
];

const operatingPrinciples = [
  "Curate the corpus first so ranking operates on a coherent technical slice.",
  "Treat explanation as part of the product, not documentation after the fact.",
  "Compare ranking runs against simple citation/date baselines before broadening the surface area."
];

export default function HomePage() {
  return (
    <main className="page">
      <section className="hero hero-grid">
        <div className="hero-copy">
          <p className="eyebrow">Research signal intelligence</p>
          <h1>Detect what matters next in audio ML.</h1>
          <p className="hero-lead">
            Research Radar is a product-shaped prototype for ranking and explaining{" "}
            <strong>MIR + audio representation learning</strong> papers. It favors
            curated scope, signal quality, and transparent reasoning over raw
            popularity or graph novelty.
          </p>
          <div className="meta">
            <span>Core slice: MIR + audio representation learning</span>
            <span>Primary data: OpenAlex</span>
            <span>Graph UI intentionally deferred</span>
          </div>
        </div>
        <aside className="hero-aside">
          {signalCards.map((card) => (
            <article key={card.label} className={`signal-card signal-card-${card.tone}`}>
              <p className="signal-card-label">{card.label}</p>
              <p className="signal-card-value">{card.value}</p>
              <p className="signal-card-copy">{card.body}</p>
            </article>
          ))}
        </aside>
      </section>

      <section className="panel section-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow eyebrow-muted">Operating model</p>
            <h2>Build the product like an instrument, not a feed</h2>
          </div>
          <div className="stamp-row">
            <span className="stamp">Curated slice only</span>
            <span className="stamp">Why surfaced matters</span>
            <span className="stamp">Evaluation before breadth</span>
          </div>
        </div>
        <div className="brief-grid">
          <article className="brief-card">
            <h3>Signal families</h3>
            <ul className="measure-list">
              {operatingPrinciples.map((principle) => (
                <li key={principle}>{principle}</li>
              ))}
            </ul>
          </article>
          <article className="brief-card">
            <h3>Current frame</h3>
            <p>
              Neural audio effects and music generation stay as a controlled
              edge slice so bridge-paper logic remains meaningful and the
              recommendation surface stays coherent.
            </p>
          </article>
        </div>
      </section>

      <section className="grid">
        {cards.map((card) => (
          <article className="card" key={card.title}>
            <h2>{card.title}</h2>
            <p>{card.body}</p>
          </article>
        ))}
      </section>

      <section className="panel section-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow eyebrow-muted">Product promise</p>
            <h2>Why this should feel different</h2>
          </div>
        </div>
        <div className="brief-grid">
          <article className="brief-card">
            <h3>Not another scholar clone</h3>
            <p>
              Search hands off into ranked views, topic momentum, and
              evaluation pages rather than collapsing every research job into a
              single endless results screen.
            </p>
          </article>
          <article className="brief-card">
            <h3>Not another graph toy</h3>
            <p>
              Bridge stays a diagnostics surface because the product explains how
              a paper connects clusters instead of expecting the graph to do the
              storytelling for us.
            </p>
          </article>
          <article className="brief-card">
            <h3>Evidence in the interface</h3>
            <p>
              Ranking weights, snapshot versions, and topic signals should read
              like instrument output, not hidden implementation detail.
            </p>
          </article>
        </div>
      </section>
    </main>
  );
}
