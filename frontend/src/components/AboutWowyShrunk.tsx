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

      <div className="about-grid about-grid--triple">
        <article className="about-card">
          <h2>How The Query Works</h2>
          <p className="about-card__subtitle">WOWY with restraint</p>
          <p>
            This metric begins with the same basic structure as WOWY: compare team outcomes with a
            player in the sample against outcomes without that player. The difference is that the
            final estimate is shrunk toward a prior so unstable rows do not dominate the board.
          </p>
          <p>
            In practice, that means players with small or uneven samples will usually move closer
            to the center than they would in plain WOWY. Players with stronger evidence keep more
            of their observed signal.
          </p>
        </article>

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
    </section>
  )
}
