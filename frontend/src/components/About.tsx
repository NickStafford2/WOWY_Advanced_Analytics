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

    </div>
  )
}
