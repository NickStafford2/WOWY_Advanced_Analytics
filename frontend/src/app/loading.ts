import type { LoadingPanelModel, LoadingPhase } from './loadingTypes'
import type { MetricId } from './metricTypes'

export function buildLoadingPanelModel({
  metric,
  metricLabel,
  isBootstrapping,
  elapsedMs,
}: {
  metric: MetricId
  metricLabel: string
  isBootstrapping: boolean
  elapsedMs: number
}): LoadingPanelModel {
  const phases = buildLoadingPhases(metric, isBootstrapping)
  const cappedProgress = isBootstrapping
    ? Math.min(72, 14 + Math.floor(elapsedMs / 180))
    : Math.min(92, 22 + Math.floor(elapsedMs / 220))
  const activePhaseIndex = Math.min(
    phases.length - 1,
    Math.floor((cappedProgress / 100) * phases.length),
  )

  return {
    title: isBootstrapping
      ? `Opening ${metricLabel} leaderboard`
      : `Loading ${metricLabel} leaderboard`,
    summary:
      phases[activePhaseIndex]?.detail ??
      `Loading ${metricLabel} data from the backend and rebuilding the leaderboard view.`,
    progressLabel: `${cappedProgress}% complete`,
    progressPercent: cappedProgress,
    phases,
    activePhaseIndex,
  }
}

function buildLoadingPhases(metric: MetricId, isBootstrapping: boolean): LoadingPhase[] {
  if (isBootstrapping) {
    return [
      {
        label: 'Reading options',
        detail: `Loading the available teams, seasons, and default filters for ${metric.toUpperCase()}.`,
      },
      {
        label: 'Checking scope',
        detail: 'Matching the current leaderboard scope against the metric store.',
      },
      {
        label: 'Preparing board',
        detail: 'Requesting the first leaderboard payload and translating it into chart and table rows.',
      },
    ]
  }

  return [
    {
      label: 'Resolving scope',
      detail: 'Applying the selected seasons, teams, and thresholds to the leaderboard request.',
    },
    {
      label: 'Checking store',
      detail:
        metric === 'rawr'
          ? 'Loading cached RAWR rows when the requested scope is already materialized.'
          : 'Loading cached WOWY rows when the requested scope is already materialized.',
    },
    {
      label: 'Building result',
      detail:
        metric === 'rawr'
          ? 'Computing the live RAWR leaderboard.'
          : 'Computing the live WOWY leaderboard.',
    },
  ]
}
