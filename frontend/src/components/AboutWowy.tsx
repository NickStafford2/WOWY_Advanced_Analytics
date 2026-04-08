export function AboutWowy() {
  return (
    <section className="about-section">
      <div className="about-section__intro">
        <span className="eyebrow">Current analytic</span>
        <h1>WOWY compares team results with a player in the sample and without them.</h1>
        <p className="lede">
          Use WOWY when you want the direct with-or-without view. The query does not try to fit a
          model. It measures how team margin changes across the filtered games when the player is
          present versus absent.
        </p>
      </div>

      <div className="about-grid">
        <article className="about-card">
          <h2>Non-Obvious Filters</h2>
          <p className="about-card__subtitle">Sample quality matters</p>
          <p>
            <strong>Min games with</strong> and <strong>Min games without</strong> are the key WOWY
            controls. They force both sides of the comparison to have enough evidence. If one side
            is too small, the apparent impact can look dramatic without being trustworthy.
          </p>
          <p>
            <strong>Min average minutes</strong> and <strong>Min total minutes</strong> cut out
            players whose rows are technically valid but not useful. Team selection also matters:
            custom team scope changes which games are allowed into both the with and without
            buckets.
          </p>
        </article>

        <article className="about-card">
          <h2>How To Read Results</h2>
          <p className="about-card__subtitle">Practical interpretation</p>
          <p>
            Start by checking whether both sides of the split are well populated. A player with a
            large difference between <strong>Avg With</strong> and <strong>Avg Without</strong> is
            only interesting if the game counts and minutes support it.
          </p>
          <p>
            WOWY is best for asking concrete sample questions, such as whether a player&apos;s teams
            consistently performed better or worse when he appeared in the filtered game set. It is
            less useful when you want the model to smooth out noisy context for you.
          </p>
        </article>
      </div>

      <section className="about-math">
        <h2>Math</h2>
        <p>
          WOWY starts from the most direct version of the question: how did the team perform in
          the filtered games when this player appeared, and how did it perform when he did not?
        </p>
        <p>
          Mathematically, that is just the difference between two sample means. The table exposes
          both sides of the split directly through <strong>With</strong>, <strong>Without</strong>,
          <strong>Avg With</strong>, and <strong>Avg Without</strong>.
        </p>
        <pre className="about-equation">
{`avg_with =
  (1 / games_with) * sum(margin_g for games g where player appears)

avg_without =
  (1 / games_without) * sum(margin_g for games g where player does not appear)

wowy_score = avg_with - avg_without`}
        </pre>
        <p>
          That is why <strong>Min games with</strong> and <strong>Min games without</strong> matter
          so much. WOWY is only as stable as the two sample means it is subtracting.
        </p>
      </section>
    </section>
  )
}
