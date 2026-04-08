export function About() {
  return (
    <section className="about-page">
      <div className="container">
        <span className="eyebrow">About RAWR Analytics</span>

        <h1>Built to turn raw NBA game data into clear, decision-ready player insights.</h1>

        <p className="lede">
          RAWR Analytics is a basketball analytics platform focused on explaining player impact
          in a way that is both technically rigorous and easy to explore. It combines data
          ingestion, metric computation, and frontend reporting into a single system designed
          for interactive analysis across seasons, teams, and custom player queries.
        </p>

        <p>
          The project centers on two core ideas: WOWY and RAWR. Together, they provide two
          complementary ways to understand performance. WOWY gives a direct, intuitive view of
          how a team performs with a player on the floor versus off the floor. RAWR goes a step
          further by estimating player impact from full game-level context, using a regression-based
          model that accounts for lineups, opponents, team environment, and sample size.
        </p>

        <div className="about-grid">
          <article className="about-card">
            <h2>WOWY</h2>
            <p className="subhead">With Or Without You</p>
            <p>
              WOWY is the fast, intuitive lens in the platform. It shows how team outcomes and
              underlying performance shift when a specific player is included versus excluded.
              This makes it useful for lineup exploration, role evaluation, and explaining impact
              in a way that is immediately understandable to both technical and non-technical users.
            </p>
            <p>
              In practice, WOWY answers questions like: How did a team perform when this player
              was on the floor? What changed when they were out? It is designed to make roster
              context visible instead of burying it inside a box score.
            </p>
          </article>

          <article className="about-card">
            <h2>RAWR</h2>
            <p className="subhead">A regression-based player impact model</p>
            <p>
              RAWR is the more advanced metric layer of the project. It is built from game-level
              observations and estimates player value through a regularized regression framework.
              The model incorporates player participation, team context, home-court effects, and
              controlled shrinkage so that results remain more stable and interpretable across
              different sample sizes.
            </p>
            <p>
              The goal of RAWR is not just to rank players, but to create a more credible estimate
              of contribution by moving beyond simple counting stats. It treats player impact as a
              modeling problem, not just a reporting problem.
            </p>
          </article>
        </div>

        <div className="about-section">
          <h2>Why this project stands out</h2>
          <p>
            RAWR Analytics is more than a dashboard. It reflects full-stack analytical product work:
            sourcing data, validating and normalizing it, building reusable metric pipelines, and
            presenting the results through a clean web interface. The platform was designed to make
            advanced sports analytics accessible without sacrificing methodological depth.
          </p>
          <p>
            From a portfolio perspective, the project demonstrates the ability to move from raw data
            to production-style user experience: data engineering, backend design, metric development,
            and frontend communication all working together in one cohesive system.
          </p>
        </div>
      </div>
    </section>
  )
}
