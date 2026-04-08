export type MetricId = 'wowy' | 'wowy_shrunk' | 'rawr'
export type ThemeMode = 'light' | 'dark'
export type LeaderboardResultSource = 'cache' | 'live'

export type TeamOption = {
  team_id: number
  label: string
  available_seasons: string[]
}

export type BaseMetricFilters = {
  team: string[] | null
  team_id: number[] | null
  season?: string[] | null
  season_type: string
  min_average_minutes: number
  min_total_minutes: number
  top_n: number
}

export type RawrMetricFilters = BaseMetricFilters & {
  min_games: number
  ridge_alpha: number
}

export type WowyMetricFilters = BaseMetricFilters & {
  min_games_with: number
  min_games_without: number
}

export type MetricFilters = RawrMetricFilters | WowyMetricFilters

export type LeaderboardFilters = {
  startSeason: string
  endSeason: string
  teamIds: number[]
  topN: number
  minGames: number
  ridgeAlpha: number
  minGamesWith: number
  minGamesWithout: number
  minAverageMinutes: number
  minTotalMinutes: number
}

export type LeaderboardNumberField =
  | 'topN'
  | 'minGames'
  | 'ridgeAlpha'
  | 'minGamesWith'
  | 'minGamesWithout'
  | 'minAverageMinutes'
  | 'minTotalMinutes'

export type SpanPoint = {
  season: string
  value: number | null
}

export type SpanSeries = {
  player_id: number
  player_name: string
  span_average_value: number
  season_count: number
  points: SpanPoint[]
}

export type ResultsTableRow = {
  rank: number
  player_id: number
  player_name: string
  span_average_value: number
  average_minutes: number | null | undefined
  total_minutes: number | null | undefined
  games?: number
  games_with?: number
  games_without?: number
  avg_margin_with?: number | null
  avg_margin_without?: number | null
  season_count: number
}

export type LeaderboardPayload = {
  mode: LeaderboardResultSource
  metric: MetricId
  span: {
    start_season: string | null
    end_season: string | null
    available_seasons: string[]
    top_n: number
  }
  table_rows: ResultsTableRow[]
  series: SpanSeries[]
  filters: MetricFilters
  available_teams?: string[]
  available_seasons?: string[]
}

export type BaseMetricOptionsPayload = {
  available_teams: string[]
  team_options: TeamOption[]
  available_seasons: string[]
  available_teams_by_season?: Record<string, string[]>
}

export type RawrMetricOptionsPayload = BaseMetricOptionsPayload & {
  metric: 'rawr'
  filters: RawrMetricFilters
}

export type WowyMetricOptionsPayload = BaseMetricOptionsPayload & {
  metric: 'wowy' | 'wowy_shrunk'
  filters: WowyMetricFilters
}

export type MetricOptionsPayload = RawrMetricOptionsPayload | WowyMetricOptionsPayload

export type ErrorPayload = {
  error?: string
}

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
