import type { MetricId } from '../app/metricTypes'

type AboutProps = {
  metric: MetricId
  metricLabel: string
  metricDescription: string
}

const ANALYTIC_DETAILS: Record<
  MetricId,
  {
    heading: string
    lede: string
    cards: {
      title: string
      subtitle: string
      body: string[]
    }[]
  }
> = {
  rawr: {
    heading: 'RAWR estimates player impact from the full game context, not just the box score.',
    lede:
      'This view is the model-driven layer of the project. It uses game-level observations and regularization to estimate how much a player moves team performance once context and sample size are accounted for.',
    cards: [
      {
        title: 'What This Screen Measures',
        subtitle: 'Regression-based player impact',
        body: [
          'RAWR starts from a ridge regression over the cached game history. The goal is to estimate player value in a way that is more stable than raw plus-minus and more informative than simple counting stats.',
          'The leaderboard reflects the currently selected seasons, teams, and minimum thresholds, so the output is always scoped to the exact slice of history on screen.',
        ],
      },
      {
        title: 'How To Read It',
        subtitle: 'Context matters',
        body: [
          'Use RAWR when you want a broader estimate of player contribution that can hold up across lineup noise and uneven samples.',
          'Ridge alpha, games thresholds, and season span all change the estimate. The screen is best read as a controlled comparison between players under the same filters.',
        ],
      },
    ],
  },
  wowy: {
    heading: 'WOWY shows how team results shift with a player in the sample and without them.',
    lede:
      'This is the direct with-or-without view. It is built for fast inspection of lineup-level impact across selected seasons and teams, using the exact filters shown beside the table.',
    cards: [
      {
        title: 'What This Screen Measures',
        subtitle: 'With Or Without You',
        body: [
          'WOWY compares team outcomes in games where the player appears against games where the player does not. It exposes the directional changes directly instead of compressing them into a single model estimate.',
          'That makes the view useful for understanding which players are tied to better or worse team performance inside the current sample window.',
        ],
      },
      {
        title: 'How To Read It',
        subtitle: 'Sample quality first',
        body: [
          'Use the games and minutes filters to avoid overreading tiny samples. The strongest rows are the ones where the pattern holds across a credible amount of playing time.',
          'This screen is most useful when you want to inspect practical on-off movement instead of asking a regression model to smooth the story.',
        ],
      },
    ],
  },
  wowy_shrunk: {
    heading: 'WOWY Shrinkage keeps the with-or-without view, but pulls small samples toward a safer prior.',
    lede:
      'This screen balances readability and restraint. It starts from WOWY-style on-off evidence, then shrinks noisy estimates so small samples do not dominate the board.',
    cards: [
      {
        title: 'What This Screen Measures',
        subtitle: 'Shrunk with-or-without impact',
        body: [
          'WOWY Shrinkage is meant for cases where direct on-off splits are informative but too volatile on their own. The estimate still comes from the with-and-without framing, but extreme values are moderated when the sample is thin.',
          'That produces a leaderboard that is easier to compare across players with uneven minutes or game coverage.',
        ],
      },
      {
        title: 'How To Read It',
        subtitle: 'Stability over drama',
        body: [
          'Use this screen when plain WOWY swings look too noisy but a full regression model is more than you want. It is the middle ground between raw on-off evidence and the broader RAWR estimate.',
          'The same season, team, and minutes filters still define the sample. Shrinkage only changes how aggressively the final value reacts to small samples.',
        ],
      },
    ],
  },
}

const SITE_DETAILS = [
  {
    title: 'What The Site Does',
    subtitle: 'A single workflow for basketball analysis',
    body: [
      'RAWR Analytics combines cached NBA data, metric computation, and a lightweight frontend into one place for interactive leaderboard analysis.',
      'The site is built to let you change the sample, inspect the result immediately, and export the filtered output without switching tools.',
    ],
  },
  {
    title: 'How The Site Is Organized',
    subtitle: 'Filters, results, and export',
    body: [
      'The left panel defines the scope: metric, season span, team scope, and minimum thresholds. The results panel applies those filters directly to the current analytic.',
      'That keeps the interface simple. You are always looking at one metric, one constrained sample, and one coherent leaderboard export.',
    ],
  },
]

export function About({ metric, metricLabel, metricDescription }: AboutProps) {
  const analyticDetails = ANALYTIC_DETAILS[metric]

  return (
    <div className="about-stack">
      <section className="about-section">
        <div className="about-section__intro">
          <span className="eyebrow">Current analytic</span>
          <h1>{analyticDetails.heading}</h1>
          <p className="lede">
            {metricLabel}: {metricDescription} {analyticDetails.lede}
          </p>
        </div>

        <div className="about-grid">
          {analyticDetails.cards.map((section) => (
            <article key={section.title} className="about-card">
              <h2>{section.title}</h2>
              <p className="about-card__subtitle">{section.subtitle}</p>
              {section.body.map((paragraph) => (
                <p key={paragraph}>{paragraph}</p>
              ))}
            </article>
          ))}
        </div>
      </section>

      <section className="about-section">
        <div className="about-section__intro">
          <span className="eyebrow">About the site</span>
          <h1>RAWR Analytics keeps the metric, the sample, and the output in one place.</h1>
          <p className="lede">
            The site is meant to be a simple analysis shell over the project data. It focuses on
            switching metrics, narrowing the sample, and reading the resulting leaderboard without
            burying the core basketball question under extra interface layers.
          </p>
        </div>

        <div className="about-grid">
          {SITE_DETAILS.map((section) => (
            <article key={section.title} className="about-card">
              <h2>{section.title}</h2>
              <p className="about-card__subtitle">{section.subtitle}</p>
              {section.body.map((paragraph) => (
                <p key={paragraph}>{paragraph}</p>
              ))}
            </article>
          ))}
        </div>
      </section>
    </div>
  )
}
