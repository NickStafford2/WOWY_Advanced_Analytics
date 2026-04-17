export type LoadingPhase = {
  label: string
  detail: string
}

export type LoadingPanelModel = {
  title: string
  summary: string
  progressLabel: string
  progressPercent: number
  phases: LoadingPhase[]
  activePhaseIndex: number
  debug?: {
    phase: string
    current: number
    total: number
  }
}

export type LeaderboardProgressEvent = {
  stream_id: string
  phase: string
  current: number
  total: number
  detail: string
  percent: number
}

export type LeaderboardResultEvent<TPayload> = {
  stream_id: string
  payload: TPayload
}

export type LeaderboardErrorEvent = {
  stream_id: string
  message: string
}
