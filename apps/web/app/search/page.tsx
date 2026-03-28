export default function SearchPage() {
  return (
    <main className="page">
      <section className="panel">
        <p className="accent-2">Search</p>
        <h1>Query papers, authors, topics, and clusters.</h1>
        <p>
          The first searchable surface will prioritize relevance and filter
          control over breadth. Users should be able to narrow by year, venue,
          cluster, and score family without feeling like they are browsing a
          generic scholar clone.
        </p>
      </section>

      <section className="split">
        <article className="panel">
          <h2>Primary filters</h2>
          <ul>
            <li>Year range</li>
            <li>Venue class and venue</li>
            <li>Cluster and topic labels</li>
            <li>Recommendation family</li>
          </ul>
        </article>
        <article className="panel">
          <h2>Search contract</h2>
          <p>
            Search should use lexical matching plus embedding retrieval, then
            hand off to ranking views instead of trying to solve every use case
            in a single results page.
          </p>
        </article>
      </section>
    </main>
  );
}
