export function About() {
  const sections = [
    {
      title: 'WOWY',
      subtitle: 'With Or Without You',
      body: [
        'WOWY is the fast, intuitive view in the platform. It shows how team outcomes move when a player is on the floor versus off it, which makes lineup context easy to explain.',
        'It answers practical questions quickly: how the team played with the player, what changed without them, and where those shifts appear across a larger sample.',
      ],
    },
    {
      title: 'RAWR',
      subtitle: 'Regression-based player impact',
      body: [
        'RAWR is the model-driven layer. It estimates player value from game-level observations using a regularized regression that accounts for lineup context, opponents, and sample size.',
        'The goal is a more credible estimate of player contribution than simple counting stats can provide, while still keeping the output readable in a web app.',
      ],
    },
  ]

  return (
    <section className="about-section">
      <div className="about-section__intro">
        <span className="eyebrow">About RAWR Analytics</span>
        <h1>Built to turn raw NBA game data into clear, decision-ready player insights.</h1>
        <p className="lede">
          RAWR Analytics is a basketball analytics platform focused on explaining player impact
          in a way that is both technically rigorous and easy to explore. It combines data
          ingestion, metric computation, and frontend reporting into a single system designed
          for interactive analysis across seasons, teams, and custom player queries.
        </p>
      </div>

      <div className="about-grid">
        {sections.map((section) => (
          <article key={section.title} className="about-card">
            <h2>{section.title}</h2>
            <p className="about-card__subtitle">{section.subtitle}</p>
            {section.body.map((paragraph) => (
              <p key={paragraph}>{paragraph}</p>
            ))}
          </article>
        ))}
      </div>

      <div className="about-callout">
        <h2>Why the project matters</h2>
        <p>
          The project is more than a dashboard. It ties together data ingestion, validation,
          metric computation, and frontend reporting in one system built for analysis instead of
          static presentation.
        </p>
        <p>
          That makes it a full-stack analytics product: the raw inputs, the statistical model,
          and the user-facing interface all have to stay coherent for the results to be useful.
        </p>
      </div>
    </section>
  )
}
