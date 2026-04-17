import type {
  LeaderboardProgressEvent,
  LoadingPanelModel,
  LoadingPhase,
} from './loadingTypes'
import type { MetricId } from './metricTypes'

type ServerLoadingConfig = {
  phases: LoadingPhase[]
  phaseIndexByKey: Record<string, number>
}

const SERVER_LOADING_CONFIGS: Record<MetricId, ServerLoadingConfig> = {
  rawr: {
    phases: [
      { label: 'Resolving scope', detail: 'Matching query filters to cached scope.' },
      { label: 'Loading cache', detail: 'Loading normalized game and player rows.' },
      { label: 'Grouping rows', detail: 'Partitioning rows by season.' },
      { label: 'Filtering seasons', detail: 'Checking complete seasons and filtering valid games.' },
      { label: 'Building result', detail: 'Computing the live RAWR leaderboard.' },
    ],
    phaseIndexByKey: {
      resolve: 0,
      scope: 0,
      'db-load': 1,
      grouping: 2,
      'season-filter': 3,
      inputs: 3,
      model: 4,
    },
  },
  wowy: {
    phases: [
      { label: 'Resolving scope', detail: 'Matching query filters to cached scope.' },
      { label: 'Loading cache', detail: 'Loading normalized game and player rows.' },
      { label: 'Preparing inputs', detail: 'Deriving season inputs from the cached rows.' },
      { label: 'Building result', detail: 'Computing the live WOWY leaderboard.' },
    ],
    phaseIndexByKey: {
      resolve: 0,
      scope: 0,
      'db-load': 1,
      inputs: 2,
      model: 3,
    },
  },
  wowy_shrunk: {
    phases: [
      { label: 'Resolving scope', detail: 'Matching query filters to cached scope.' },
      { label: 'Loading cache', detail: 'Loading normalized game and player rows.' },
      { label: 'Preparing inputs', detail: 'Deriving season inputs from the cached rows.' },
      { label: 'Building result', detail: 'Computing the live WOWY Shrinkage leaderboard.' },
    ],
    phaseIndexByKey: {
      resolve: 0,
      scope: 0,
      'db-load': 1,
      inputs: 2,
      model: 3,
    },
  },
}

const REQUEST_LOADING_PHASES: Record<MetricId, LoadingPhase[]> = {
  rawr: [
    {
      label: 'Resolving scope',
      detail: 'Applying the selected seasons, teams, and thresholds to the leaderboard request.',
    },
    {
      label: 'Checking store',
      detail: 'Loading cached RAWR rows when the requested scope is already materialized.',
    },
    {
      label: 'Building result',
      detail: 'Computing the live RAWR leaderboard.',
    },
  ],
  wowy: [
    {
      label: 'Resolving scope',
      detail: 'Applying the selected seasons, teams, and thresholds to the leaderboard request.',
    },
    {
      label: 'Checking store',
      detail: 'Loading cached WOWY rows when the requested scope is already materialized.',
    },
    {
      label: 'Building result',
      detail: 'Computing the live WOWY leaderboard.',
    },
  ],
  wowy_shrunk: [
    {
      label: 'Resolving scope',
      detail: 'Applying the selected seasons, teams, and thresholds to the leaderboard request.',
    },
    {
      label: 'Checking store',
      detail: 'Loading cached WOWY rows when the requested scope is already materialized.',
    },
    {
      label: 'Building result',
      detail: 'Computing the live WOWY leaderboard.',
    },
  ],
}

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
  const config = SERVER_LOADING_CONFIGS[metric]

  const activePhaseIndex = config.phaseIndexByKey[progress.phase] ?? 0

  return {
    title: `Loading ${metricLabel} leaderboard`,
    summary: progress.detail,
    progressLabel: `${progress.percent}% complete`,
    progressPercent: progress.percent,
    phases: config.phases,
    activePhaseIndex,
    debug: {
      phase: progress.phase,
      current: progress.current,
      total: progress.total,
    },
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

  return REQUEST_LOADING_PHASES[metric]
}
