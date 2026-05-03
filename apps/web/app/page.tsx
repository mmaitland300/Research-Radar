const cards = [
  {
    title: "Emerging Papers",
    body:
      "Rank papers by citation velocity and local topic growth, with semantic fields shown only when a selected run computes and uses them."
  },
  {
    title: "Bridge preview",
    body:
      "Inspect cross-cluster candidates with measured bridge signal. Bridge stays in review mode until labeled review, proxy evaluation, and product policy support stronger recommender claims."
  },
  {
    title: "Explainable Ranking",
    body:
      "Expose score contributions, cluster context, and baseline comparisons so each recommendation feels engineered."
  },
  {
    title: "Evaluation First",
    body:
      "Pair proxy evaluation with baseline comparison so ranking behavior can be inspected before stronger quality claims are made."
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
      "Inspect candidate works that may connect nearby but distinct research neighborhoods, with bridge signal kept in review mode until labeled review, proxy evaluation, and product policy support stronger claims."
  },
  {
    tone: "undercited",
    label: "Undercited",
    value: "Early signal",
    body: "Watch low-citation candidates that look stronger than their current attention suggests."
  }
];

const operatingPrinciples = [
  "Curate the corpus first so ranking operates on coherent technical scope.",
  "Treat explanation as part of the product, not documentation after the fact.",
  "Compare ranking runs against simple citation/date baselines before broadening the public views."
];

export default function HomePage() {
  return (
    <main className="page">
      <section className="hero hero-grid">
        <div className="hero-copy">
          <p className="eyebrow">Audio ML paper discovery</p>
          <h1>Detect what matters next in audio ML.</h1>
          <p className="hero-lead">
            Research Radar is a working prototype for ranking and explaining{" "}
            <strong>MIR + audio representation learning</strong> papers. It keeps
            the corpus narrow, shows the ranking signals, and explains why each
            paper appears.
          </p>
          <div className="meta">
            <span>Core corpus: MIR + audio representation learning</span>
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
            <p className="eyebrow eyebrow-muted">Product frame</p>
            <h2>Build a ranking instrument for a narrow corpus</h2>
          </div>
          <div className="stamp-row">
            <span className="stamp">Curated corpus</span>
            <span className="stamp">Why each paper appears</span>
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
              edge set so bridge-paper logic remains meaningful and the
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
            <h2>How it differs in use</h2>
          </div>
        </div>
        <div className="brief-grid">
          <article className="brief-card">
            <h3>Search that hands off</h3>
            <p>
              Search hands off into ranked views, topic momentum, and
              evaluation pages, so discovery work does not end at a single
              results screen.
            </p>
          </article>
          <article className="brief-card">
            <h3>Bridge with evidence</h3>
            <p>
              Bridge stays in review mode while the product explains how a paper
              connects clusters and what evidence the current run recorded.
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
