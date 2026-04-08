export function AboutRawr() {
  return (
    <section className="about-section">
      <div className="about-section__intro">
        <span className="eyebrow">Current analytic</span>
        <h1>RAWR estimates player impact with a ridge regression over game-level results.</h1>
        <p className="lede">
          Use RAWR when you want a model-based estimate of player value instead of a direct
          with-or-without split. The query takes the selected seasons and team scope, builds a
          game-level sample, and estimates how strongly each player is associated with team
          performance after the model accounts for the rest of the lineup context.
        </p>
      </div>

      <div className="about-grid about-grid--triple">
        <article className="about-card">
          <h2>How The Query Works</h2>
          <p className="about-card__subtitle">Regression basics</p>
          <p>
            Regression is a way to explain an observed outcome by fitting weights to a set of
            inputs. Here, the outcome is team performance at the game level, and the inputs include
            which players were involved in the sample.
          </p>
          <p>
            The model tries to find a set of player values that best explains the observed results
            across the filtered games. The leaderboard is not showing raw box score production. It
            is showing the model&apos;s estimate of each player&apos;s impact in the selected sample.
          </p>
        </article>

        <article className="about-card">
          <h2>Non-Obvious Filters</h2>
          <p className="about-card__subtitle">What changes the estimate</p>
          <p>
            <strong>Ridge alpha</strong> controls how aggressively the regression shrinks player
            estimates toward zero. Higher values make the model more conservative and reduce noisy
            extremes. Lower values let the fitted values react more strongly to the data.
          </p>
          <p>
            <strong>Min games</strong> removes players with too little game coverage before the
            leaderboard is returned. <strong>Min average minutes</strong> and{' '}
            <strong>Min total minutes</strong> are often the more important filters, because they
            stop the board from being dominated by players whose estimate comes from a thin minute
            sample.
          </p>
        </article>

        <article className="about-card">
          <h2>How To Read Results</h2>
          <p className="about-card__subtitle">Interpretation</p>
          <p>
            Read RAWR as a comparative estimate inside the exact query you ran. If you change the
            season span, teams, or thresholds, you are changing the evidence the model is allowed
            to learn from.
          </p>
          <p>
            Start with players who remain strong under stricter minute and game filters. Those are
            usually the more credible signals. Use the export when you want to compare multiple
            query settings side by side rather than reading a single board in isolation.
          </p>
        </article>
      </div>
    </section>
  )
}
