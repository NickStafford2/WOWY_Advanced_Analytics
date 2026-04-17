import type {
  LeaderboardProgressEvent,
  LoadingPanelModel,
  LoadingPhase,
} from './loadingTypes'
import type { MetricId } from './metricTypes'

export function buildLoadingPanelModel({
  metric,
  metricLabel,
  isBootstrapping,
  elapsedMs,
  serverProgress,
}: {
  metric: MetricId
  metricLabel: string
  isBootstrapping: boolean
  elapsedMs: number
  serverProgress?: LeaderboardProgressEvent | null
}): LoadingPanelModel {
  if (serverProgress != null) {
    return buildServerLoadingPanelModel({
      metricLabel,
      progress: serverProgress,
    })
  }

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

function buildServerLoadingPanelModel({
  metricLabel,
  progress,
}: {
  metricLabel: string
  progress: LeaderboardProgressEvent
}): LoadingPanelModel {
  const phases: LoadingPhase[] = [
    { label: 'Resolving scope', detail: 'Matching query filters to cached scope.' },
    { label: 'Loading cache', detail: 'Loading normalized game and player rows.' },
    { label: 'Grouping rows', detail: 'Partitioning rows by season.' },
    { label: 'Filtering seasons', detail: 'Checking complete seasons and filtering valid games.' },
    { label: 'Building result', detail: 'Computing the live RAWR leaderboard.' },
  ]

  const phaseIndexByKey: Record<string, number> = {
    resolve: 0,
    scope: 0,
    'db-load': 1,
    grouping: 2,
    'season-filter': 3,
    inputs: 3,
    model: 4,
  }

  const activePhaseIndex = phaseIndexByKey[progress.phase] ?? 0

  return {
    title: `Loading ${metricLabel} leaderboard`,
    summary: progress.detail,
    progressLabel: `${progress.percent}% complete`,
    progressPercent: progress.percent,
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
