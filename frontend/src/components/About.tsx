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
    <div className="mt-5 flex flex-col gap-5">
      {metricSection}

    </div>
  )
}
