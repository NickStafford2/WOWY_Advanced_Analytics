import type { MetricId } from './types'

type MetricOption = {
  id: MetricId
  label: string
}

const METRIC_OPTIONS: MetricOption[] = [
  { id: 'rawr', label: 'RAWR' },
  { id: 'wowy_shrunk', label: 'WOWY Shrinkage' },
  { id: 'wowy', label: 'WOWY' },
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

export function metricOptions(): MetricOption[] {
  return METRIC_OPTIONS
}
