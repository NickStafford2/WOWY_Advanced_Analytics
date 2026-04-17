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
  const phases = buildLoadingPhases(metric, isBootstrapping)

  if (serverProgress != null) {
    return buildServerLoadingPanelModel({
      metric,
      metricLabel,
      progress: serverProgress,
    })
  }

  return {
    title: isBootstrapping
      ? `Opening ${metricLabel} leaderboard`
      : `Connecting to ${metricLabel} stream`,
    summary: isBootstrapping
      ? `Loading the initial ${metricLabel} view.`
      : `Waiting for the server to send progress events.`,
    progressLabel: 'Waiting for server progress',
    progressPercent: 0,
    phases,
    activePhaseIndex: 0,
  }
}

function buildServerLoadingPanelModel({
  metric,
  metricLabel,
  progress,
}: {
  metric: MetricId
  metricLabel: string
  progress: LeaderboardProgressEvent
}): LoadingPanelModel {
  const phases = metric === 'rawr'
    ? buildRawrServerLoadingPhases()
    : buildWowyServerLoadingPhases(metric)
  const phaseIndexByKey = metric === 'rawr'
    ? {
      resolve: 0,
      scope: 0,
      'db-load': 1,
      grouping: 2,
      'season-filter': 3,
      inputs: 3,
      model: 4,
    }
    : {
      resolve: 0,
      scope: 0,
      'db-load': 1,
      inputs: 2,
      model: 3,
    }

  const activePhaseIndex = phaseIndexByKey[progress.phase] ?? 0

  return {
    title: `Loading ${metricLabel} leaderboard`,
    summary: progress.detail,
    progressLabel: `${progress.percent}% complete`,
    progressPercent: progress.percent,
    phases,
    activePhaseIndex,
    debug: {
      phase: progress.phase,
      current: progress.current,
      total: progress.total,
    },
  }
}

function buildRawrServerLoadingPhases(): LoadingPhase[] {
  return [
    { label: 'Resolving scope', detail: 'Matching query filters to cached scope.' },
    { label: 'Loading cache', detail: 'Loading normalized game and player rows.' },
    { label: 'Grouping rows', detail: 'Partitioning rows by season.' },
    { label: 'Filtering seasons', detail: 'Checking complete seasons and filtering valid games.' },
    { label: 'Building result', detail: 'Computing the live RAWR leaderboard.' },
  ]
}

function buildWowyServerLoadingPhases(metric: MetricId): LoadingPhase[] {
  const metricLabel = metric === 'wowy_shrunk' ? 'WOWY Shrinkage' : 'WOWY'

  return [
    { label: 'Resolving scope', detail: 'Matching query filters to cached scope.' },
    { label: 'Loading cache', detail: 'Loading normalized game and player rows.' },
    { label: 'Preparing inputs', detail: 'Deriving season inputs from the cached rows.' },
    { label: 'Building result', detail: `Computing the live ${metricLabel} leaderboard.` },
  ]
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
