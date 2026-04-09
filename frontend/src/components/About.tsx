import type { MetricId } from '../app/metricTypes'
import { AboutRawr } from './AboutRawr'
import { AboutWowy } from './AboutWowy'
import { AboutWowyShrunk } from './AboutWowyShrunk'

const PANEL_LABEL_CLASS_NAME =
  'm-0 text-xs font-bold tracking-[0.16em] uppercase text-[color:var(--accent-warm)]'

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
    <div className="mt-5 flex flex-col gap-5">
      {metricSection}

      <section className="rounded-[28px] border border-[color:var(--panel-border)] bg-[var(--panel-soft-background)] p-6 shadow-[var(--panel-shadow)] max-sm:rounded-[22px] max-sm:p-[18px]">
        <div className="flex flex-col gap-[14px]">
          <span className={PANEL_LABEL_CLASS_NAME}>About the site</span>
          <h1 className="text-[clamp(2rem,4vw,3rem)] leading-none">
            RAWR Analytics keeps the metric, the sample, and the output in one place.
          </h1>
          <p className="m-0 max-w-[68ch] text-[1.02rem] leading-[1.65] text-[color:var(--text-muted)]">
            The site is meant to be a simple analysis shell over the project data. It focuses on
            switching metrics, narrowing the sample, and reading the resulting leaderboard without
            burying the core basketball question under extra interface layers.
          </p>
        </div>

        <div className="mt-5 grid grid-cols-2 gap-4 max-[1120px]:grid-cols-1">
          <article className="rounded-3xl border border-[color:var(--panel-border-soft)] bg-[var(--panel-card-background)] p-5">
            <h2 className="m-0 text-[1.4rem] leading-[1.15]">What The Site Does</h2>
            <p className="mt-2 mb-3 font-bold text-[color:var(--text-soft)]">
              A single workflow for basketball analysis
            </p>
            <p className="m-0 leading-[1.65] text-[color:var(--text-muted)]">
              RAWR Analytics combines cached NBA data, metric computation, and a lightweight
              frontend into one place for interactive leaderboard analysis.
            </p>
            <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
              The site is built to let you change the sample, inspect the result immediately, and
              export the filtered output without switching tools.
            </p>
          </article>

          <article className="rounded-3xl border border-[color:var(--panel-border-soft)] bg-[var(--panel-card-background)] p-5">
            <h2 className="m-0 text-[1.4rem] leading-[1.15]">How The Site Is Organized</h2>
            <p className="mt-2 mb-3 font-bold text-[color:var(--text-soft)]">
              Filters, results, and export
            </p>
            <p className="m-0 leading-[1.65] text-[color:var(--text-muted)]">
              The left panel defines the scope: metric, season span, team scope, and minimum
              thresholds. The results panel applies those filters directly to the current analytic.
            </p>
            <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
              That keeps the interface simple. You are always looking at one metric, one
              constrained sample, and one coherent leaderboard export.
            </p>
          </article>
        </div>
      </section>
    </div>
  )
}
