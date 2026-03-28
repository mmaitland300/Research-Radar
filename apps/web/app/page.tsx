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

export default function HomePage() {
  return (
    <>
      <section className="hero">
        <p className="accent">Research Radar</p>
        <h1>Emerging and bridge papers in audio ML.</h1>
        <p>
          V1 is a ranking and explainability product for <strong>MIR + audio
          representation learning</strong>. Neural audio effects and music
          generation are held back as a controlled edge slice so the corpus
          stays coherent while bridge-paper logic stays meaningful.
        </p>
        <div className="meta">
          <span>Core slice: MIR + audio representation learning</span>
          <span>Primary data: OpenAlex</span>
          <span>V1 excludes graph UI</span>
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
    </>
  );
}
