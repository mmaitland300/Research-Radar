const cards = [
  {
    title: "Emerging Papers",
    body:
      "Rank papers by semantic fit, citation velocity, and local topic growth instead of raw popularity alone."
  },
  {
    title: "Bridge Detection",
    body:
      "Surface work connecting nearby but distinct audio-ML clusters without turning the product into a graph toy."
  },
  {
    title: "Explainable Ranking",
    body:
      "Expose score contributions, cluster context, and baseline comparisons so each recommendation feels engineered."
  },
  {
    title: "Evaluation First",
    body:
      "Ship benchmark, diversity, and temporal backtest pages as part of MVP rather than as an afterthought."
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
    body: "Surface works that connect nearby but distinct research neighborhoods without falling back to a graph toy."
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
  "Measure with benchmark and baseline views before broadening the surface area."
];

export default function HomePage() {
  return (
    <main className="page">
      <section className="hero hero-grid">
        <div className="hero-copy">
          <p className="eyebrow">Research signal intelligence</p>
          <h1>Detect what matters next in audio ML.</h1>
          <p className="hero-lead">
            Research Radar is a ranking and explainability product for{" "}
            <strong>MIR + audio representation learning</strong>. It favors
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
            <span className="stamp">Benchmark before breadth</span>
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
              Bridge detection stays useful because the product explains how a
              paper connects clusters instead of expecting the graph to do the
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
