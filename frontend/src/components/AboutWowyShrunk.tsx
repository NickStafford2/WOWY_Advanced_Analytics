export function AboutWowyShrunk() {
  return (
    <section className="about-section">
      <div className="about-section__intro">
        <span className="eyebrow">Current analytic</span>
        <h1>WOWY Shrinkage starts from WOWY, then pulls unstable estimates toward a safer middle.</h1>
        <p className="lede">
          Use WOWY Shrinkage when plain WOWY is directionally useful but too volatile. The query
          still depends on with-and-without samples, but it softens extreme results when the data
          behind them is thin.
        </p>
      </div>

      <div className="about-grid">
        <article className="about-card">
          <h2>How It Compares To WOWY</h2>
          <p className="about-card__subtitle">Same idea, steadier output</p>
          <p>
            Plain WOWY reports the direct with-and-without difference as observed in the query
            sample. WOWY Shrinkage uses that same evidence but discounts the most fragile edges of
            the board.
          </p>
          <p>
            If a player looks extreme in WOWY but ordinary here, that is usually a sign that the
            WOWY result was driven by limited evidence. If a player remains strong in both views,
            the signal is more likely to be robust.
          </p>
        </article>

        <article className="about-card">
          <h2>Non-Obvious Filters</h2>
          <p className="about-card__subtitle">Still driven by the sample</p>
          <p>
            <strong>Min games with</strong> and <strong>Min games without</strong> still matter,
            even though shrinkage stabilizes the output. Shrinkage is not a substitute for sample
            discipline. It only makes thin samples less explosive.
          </p>
          <p>
            <strong>Min average minutes</strong>, <strong>Min total minutes</strong>, and custom
            team scope all still change the underlying evidence. The most useful workflow is to
            compare this board against plain WOWY under the same filters and see where the ranking
            meaningfully tightens.
          </p>
        </article>
      </div>

      <section className="about-math">
        <h2>Math</h2>
        <p>
          WOWY Shrinkage still begins with the plain WOWY question: compare team margin with the
          player against team margin without the player. The difference is that the final score is
          pulled back toward zero when the evidence is thin.
        </p>
        <p>
          The general idea is shrinkage. Shrinkage leaves strong, well-supported estimates closer
          to their observed value and compresses weaker estimates toward a safer middle. Here that
          compression depends on the harmonic mean of the with and without game counts.
        </p>
        <pre className="about-equation">
{`wowy_score = avg_with - avg_without

effective_games =
  (2 * games_with * games_without) / (games_with + games_without)

shrinkage_factor =
  effective_games / (effective_games + prior_games)

shrunk_score = wowy_score * shrinkage_factor`}
        </pre>
        <p>
          In the current backend, <code>prior_games</code> defaults to <code>10</code>. If the with
          and without samples are both large, the shrinkage factor moves toward <code>1</code> and
          the result stays close to plain WOWY. If either side is small, the factor drops and the
          score is pulled toward zero.
        </p>
      </section>
    </section>
  )
}
