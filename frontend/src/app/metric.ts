import type { MetricId } from './metricTypes'

type MetricOption = {
  id: MetricId
  label: string
  standsFor: string
}

const METRIC_OPTIONS: MetricOption[] = [
  { id: 'rawr', label: 'RAWR', standsFor: 'Real Adjusted WOWY Regression' },
  { id: 'wowy_shrunk', label: 'WOWY Shrinkage', standsFor: 'With Or Without You, with shrinkage toward the prior' },
  { id: 'wowy', label: 'WOWY', standsFor: 'With Or Without You' },
]

export function metricLabelFor(metric: MetricId): string {
  const metricOption = METRIC_OPTIONS.find((option) => option.id === metric)
  if (metricOption === undefined) {
    return 'WOWY'
  }

  return metricOption.label
}

export function metricDescriptionFor(metric: MetricId): string {
  if (metric === 'wowy') {
    return 'Cross-season on and off impact from with-and-without samples.'
  }
  if (metric === 'wowy_shrunk') {
    return 'WOWY impact with shrinkage applied so smaller samples pull toward the prior.'
  }

  return 'Game-level ridge model of player impact across the cached history.'
}

export function metricStandsFor(metric: MetricId): string {
  const metricOption = METRIC_OPTIONS.find((option) => option.id === metric)
  if (metricOption === undefined) {
    return 'With Or Without You'
  }

  return metricOption.standsFor
}

export function metricOptions(): MetricOption[] {
  return METRIC_OPTIONS
}
