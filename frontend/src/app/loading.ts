import type { AppMode, LoadingPanelModel, LoadingPhase, MetricId } from './types'

export function buildLoadingPanelModel({
  metric,
  metricLabel,
  mode,
  isBootstrapping,
  elapsedMs,
}: {
  metric: MetricId
  metricLabel: string
  mode: AppMode
  isBootstrapping: boolean
  elapsedMs: number
}): LoadingPanelModel {
  const phases = buildLoadingPhases(metric, mode, isBootstrapping)
  const cappedProgress = isBootstrapping
    ? Math.min(72, 14 + Math.floor(elapsedMs / 180))
    : Math.min(92, 22 + Math.floor(elapsedMs / 220))
  const activePhaseIndex = Math.min(
    phases.length - 1,
    Math.floor((cappedProgress / 100) * phases.length),
  )
  const title = isBootstrapping
    ? `Opening ${metricLabel} data pipeline`
    : mode === 'custom'
      ? `Running ${metricLabel} custom query`
      : `Refreshing cached ${metricLabel} leaderboard`

  return {
    title,
    summary:
      phases[activePhaseIndex]?.detail ??
      `Loading ${metricLabel} data from the backend and rebuilding the chart payload.`,
    progressLabel: `${cappedProgress}% complete`,
    progressPercent: cappedProgress,
    phases,
    activePhaseIndex,
  }
}

function buildLoadingPhases(
  metric: MetricId,
  mode: AppMode,
  isBootstrapping: boolean,
): LoadingPhase[] {
  if (isBootstrapping) {
    return [
      {
        label: 'Inspecting scope',
        detail: `Checking which cached teams and seasons are available for ${metric.toUpperCase()}.`,
      },
      {
        label: 'Reading defaults',
        detail: 'Loading the recommended filters so the first render matches the current metric store.',
      },
      {
        label: 'Preparing board',
        detail: 'Requesting the first leaderboard payload and translating it into chart-ready series.',
      },
    ]
  }

  if (metric === 'rawr') {
    if (mode === 'custom') {
      return [
        {
          label: 'Gathering sample',
          detail: 'Collecting the requested team and season slice from the normalized RAWR inputs.',
        },
        {
          label: 'Fitting ridge',
          detail: 'Running the game-level ridge regression with the selected alpha and minimum games threshold.',
        },
        {
          label: 'Ranking span',
          detail: 'Aggregating the player-season coefficients into the final span leaderboard and chart points.',
        },
      ]
    }

    return [
      {
        label: 'Loading scope',
        detail: 'Reading the prebuilt RAWR regression store for the selected team scope and season type.',
      },
      {
        label: 'Filtering rows',
        detail: 'Applying the minimum games and minute thresholds before ranking the remaining player seasons.',
      },
      {
        label: 'Rendering chart',
        detail: 'Rebuilding the multi-season series and ranked table for the frontend.',
      },
    ]
  }

  if (mode === 'custom') {
    return [
      {
        label: 'Gathering sample',
        detail:
          metric === 'wowy_shrunk'
            ? 'Collecting the requested team and season slice from cached WOWY inputs before shrinkage is applied.'
            : 'Collecting the requested team and season slice from cached WOWY inputs.',
      },
      {
        label: metric === 'wowy_shrunk' ? 'Applying shrinkage' : 'Running WOWY',
        detail:
          metric === 'wowy_shrunk'
            ? 'Computing with/without impact and shrinking each player-season toward the prior based on sample balance.'
            : 'Computing with/without impact for each player across the selected game sample.',
      },
      {
        label: 'Ranking span',
        detail: 'Aggregating the player-season results into the final span leaderboard and chart points.',
      },
    ]
  }

  return [
    {
      label: 'Loading cache',
      detail:
        metric === 'wowy_shrunk'
          ? 'Reading cached WOWY shrinkage player-season rows for the selected scope.'
          : 'Reading cached WOWY player-season rows for the selected scope.',
    },
    {
      label: 'Applying filters',
      detail: 'Filtering by minutes and sample sizes before ranking the strongest multi-season profiles.',
    },
    {
      label: 'Rendering board',
      detail: 'Building the chart series and leaderboard table for the current span.',
    },
  ]
}
