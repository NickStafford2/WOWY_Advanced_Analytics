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

      <div className="about-grid">
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

      <section className="about-math">
        <h2>Math</h2>
        <p>
          RAWR asks a simple question first: what set of player values best explains the game
          margins in the filtered sample? Instead of comparing with and without buckets directly,
          it fits one model across all selected games at once.
        </p>
        <p>
          The general idea is regression. Regression fits coefficients so predicted outcomes stay as
          close as possible to observed outcomes. Here the observed outcome is game margin, and the
          model uses player terms plus a few context terms to explain it.
        </p>
        <pre className="about-equation">
{`predicted_margin_i =
  beta_0
  + beta_home * home_sign_i
  + sum(x_i,p * beta_p for players p)
  + gamma_team(i)
  + delta_opponent(i)`}
        </pre>
        <p>
          The fit uses ridge regression, which means ordinary squared-error fitting plus a penalty
          that pulls coefficients back toward zero. That penalty is what keeps the model from
          chasing every noisy fluctuation in the sample.
        </p>
        <pre className="about-equation">
{`beta_hat = argmin over beta:
  sum((margin_i - predicted_margin_i)^2 over observations i)
  + ridge_alpha * sum(beta_p^2 over player terms p)
  + ridge_alpha * sum(gamma_t^2 over team-season terms t)
  + ridge_alpha * sum(delta_t^2 over opponent terms t)`}
        </pre>
        <p>
          Bigger <strong>Ridge alpha</strong> means more shrinkage toward zero and therefore more
          conservative player estimates. Smaller values let the model follow the sample more
          aggressively. The leaderboard value for a player is that player&apos;s fitted coefficient.
        </p>
      </section>
    </section>
  )
}
