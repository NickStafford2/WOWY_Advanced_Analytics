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
}
