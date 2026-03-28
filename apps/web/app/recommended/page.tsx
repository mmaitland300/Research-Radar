const families = [
  {
    title: "Emerging",
    detail: "High local growth with strong relevance before consensus citations harden."
  },
  {
    title: "Bridge",
    detail: "Unusually well-positioned across nearby but distinct clusters."
  },
  {
    title: "Under-cited but Relevant",
    detail: "Technically aligned work that beats citation-only baselines on fit and novelty."
  }
];

export default function RecommendedPage() {
  return (
    <main className="page">
      <section className="panel">
        <p className="accent">Recommended</p>
        <h1>Ranking families, not one undifferentiated feed.</h1>
        <p>
          Recommendations are materialized by ranking run so the UI can say
          exactly which corpus snapshot, embedding set, and weight profile
          produced a given list.
        </p>
      </section>

      <section className="grid">
        {families.map((family) => (
          <article className="card" key={family.title}>
            <h2>{family.title}</h2>
            <p>{family.detail}</p>
          </article>
        ))}
      </section>
    </main>
  );
}
