import type { MetricId } from '../app/metricTypes'
import { AboutRawr } from './AboutRawr'
import { AboutWowy } from './AboutWowy'
import { AboutWowyShrunk } from './AboutWowyShrunk'

type AboutProps = {
  metric: MetricId
}

export function About({ metric }: AboutProps) {
  const metricSection = metric === 'rawr'
    ? <AboutRawr />
    : metric === 'wowy_shrunk'
      ? <AboutWowyShrunk />
      : <AboutWowy />

  return (
    <div className="about-stack">
      {metricSection}

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
          <article className="about-card">
            <h2>What The Site Does</h2>
            <p className="about-card__subtitle">A single workflow for basketball analysis</p>
            <p>
              RAWR Analytics combines cached NBA data, metric computation, and a lightweight
              frontend into one place for interactive leaderboard analysis.
            </p>
            <p>
              The site is built to let you change the sample, inspect the result immediately, and
              export the filtered output without switching tools.
            </p>
          </article>

          <article className="about-card">
            <h2>How The Site Is Organized</h2>
            <p className="about-card__subtitle">Filters, results, and export</p>
            <p>
              The left panel defines the scope: metric, season span, team scope, and minimum
              thresholds. The results panel applies those filters directly to the current analytic.
            </p>
            <p>
              That keeps the interface simple. You are always looking at one metric, one
              constrained sample, and one coherent leaderboard export.
            </p>
          </article>
        </div>
      </section>
    </div>
  )
}
