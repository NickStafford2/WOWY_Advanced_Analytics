import type {
  CachedFilters,
  CustomFilters,
  MetricFilters,
  MetricId,
  MetricNumberField,
  MetricOptionsPayload,
  RawrMetricFilters,
  TeamOption,
  WowyMetricFilters,
} from './types'

export function defaultMetricFilters(metric: MetricId): MetricFilters {
  if (metric === 'rawr') {
    return {
      team: null,
      team_id: null,
      season_type: 'Regular Season',
      min_games: 35,
      ridge_alpha: 10,
      min_average_minutes: 30,
      min_total_minutes: 600,
      top_n: 30,
    }
  }

  return {
    team: null,
    team_id: null,
    season_type: 'Regular Season',
    min_games_with: 15,
    min_games_without: 2,
    min_average_minutes: 30,
    min_total_minutes: 600,
    top_n: 30,
  }
}

export function defaultCustomFilters(): CustomFilters {
  return {
    startSeason: '',
    endSeason: '',
    teams: [],
    topN: 12,
    minGames: 35,
    ridgeAlpha: 10,
    minGamesWith: 15,
    minGamesWithout: 2,
    minAverageMinutes: 30,
    minTotalMinutes: 600,
  }
}

export function metricLabelFor(metric: MetricId): string {
  if (metric === 'rawr') {
    return 'RAWR'
  }
  if (metric === 'wowy_shrunk') {
    return 'WOWY Shrinkage'
  }
  return 'WOWY'
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

export function syncCustomFiltersWithOptions(
  current: CustomFilters,
  payload: MetricOptionsPayload,
): CustomFilters {
  const defaultStartSeason = payload.available_seasons[0] || ''
  const defaultEndSeason = payload.available_seasons[payload.available_seasons.length - 1] || ''
  const nextTeams = current.teams.filter((teamId) =>
    payload.team_options.some((teamOption) => teamOption.team_id === teamId),
  )

  return {
    ...current,
    startSeason: payload.available_seasons.includes(current.startSeason)
      ? current.startSeason
      : defaultStartSeason,
    endSeason: payload.available_seasons.includes(current.endSeason)
      ? current.endSeason
      : defaultEndSeason,
    teams:
      nextTeams.length > 0
        ? nextTeams
        : payload.team_options.map((teamOption) => teamOption.team_id),
    topN: current.topN || payload.filters.top_n,
    minGames: payload.metric === 'rawr' ? payload.filters.min_games : current.minGames,
    ridgeAlpha: payload.metric === 'rawr' ? payload.filters.ridge_alpha : current.ridgeAlpha,
    minGamesWith: payload.metric === 'rawr' ? current.minGamesWith : payload.filters.min_games_with,
    minGamesWithout:
      payload.metric === 'rawr' ? current.minGamesWithout : payload.filters.min_games_without,
    minAverageMinutes: payload.filters.min_average_minutes,
    minTotalMinutes: payload.filters.min_total_minutes,
  }
}

export function seasonSpan(
  startSeason: string,
  endSeason: string,
  seasons: string[],
): string[] {
  if (!startSeason || !endSeason) {
    return []
  }

  const startIndex = seasons.indexOf(startSeason)
  const endIndex = seasons.indexOf(endSeason)
  if (startIndex === -1 || endIndex === -1) {
    return []
  }

  const lowIndex = Math.min(startIndex, endIndex)
  const highIndex = Math.max(startIndex, endIndex)
  return seasons.slice(lowIndex, highIndex + 1)
}

export function buildAvailableTeamsForSeasonSpan({
  startSeason,
  endSeason,
  availableSeasons,
  teamOptions,
}: {
  startSeason: string
  endSeason: string
  availableSeasons: string[]
  teamOptions: TeamOption[]
}): TeamOption[] {
  const seasonsInScope = seasonSpan(startSeason, endSeason, availableSeasons)
  if (seasonsInScope.length === 0) {
    return []
  }

  return teamOptions.filter((teamOption) =>
    seasonsInScope.every((season) => teamOption.available_seasons.includes(season)),
  )
}

export function filterSelectedTeamIdsForAvailableTeams(
  selectedTeamIds: number[],
  availableTeams: TeamOption[],
): number[] {
  const availableTeamIds = new Set(availableTeams.map((team) => team.team_id))
  return selectedTeamIds.filter((teamId) => availableTeamIds.has(teamId))
}

export function syncSelectedTeamIds(
  selectedTeamIds: number[],
  availableTeams: TeamOption[],
): number[] {
  return filterSelectedTeamIdsForAvailableTeams(selectedTeamIds, availableTeams)
}

export function toggleSelectedTeam(selectedTeamIds: number[], teamId: number): number[] {
  if (selectedTeamIds.includes(teamId)) {
    return selectedTeamIds.filter((selectedTeamId) => selectedTeamId !== teamId)
  }
  return [...selectedTeamIds, teamId]
}

export function toggleAllSelectedTeams(
  selectedTeamIds: number[],
  availableTeams: TeamOption[],
): number[] {
  if (selectedTeamIds.length === availableTeams.length) {
    return []
  }
  return availableTeams.map((team) => team.team_id)
}

export function updateMetricFilterValue(
  filters: MetricFilters,
  field: MetricNumberField,
  value: number,
): MetricFilters {
  const nextValue = sanitizeNumber(value)

  if (field === 'minAverageMinutes') {
    return { ...filters, min_average_minutes: nextValue }
  }
  if (field === 'minTotalMinutes') {
    return { ...filters, min_total_minutes: nextValue }
  }
  if (field === 'minGames' && 'min_games' in filters) {
    return { ...filters, min_games: nextValue }
  }
  if (field === 'ridgeAlpha' && 'ridge_alpha' in filters) {
    return { ...filters, ridge_alpha: nextValue }
  }
  if (field === 'minGamesWith' && 'min_games_with' in filters) {
    return { ...filters, min_games_with: nextValue }
  }
  if (field === 'minGamesWithout' && 'min_games_without' in filters) {
    return { ...filters, min_games_without: nextValue }
  }

  return filters
}

export function buildExportUrl({
  metric,
  mode,
  cachedFilters,
  customFilters,
  availableSeasons,
  availableCustomTeams,
  metricFilters,
}: {
  metric: MetricId
  mode: 'cached' | 'custom'
  cachedFilters: CachedFilters
  customFilters: CustomFilters
  availableSeasons: string[]
  availableCustomTeams: TeamOption[]
  metricFilters: MetricFilters
}): string {
  if (mode === 'cached') {
    return `/api/metrics/${metric}/cached-leaderboard.csv?${buildCachedLeaderboardParams(
      metric,
      cachedFilters,
      metricFilters,
    ).toString()}`
  }

  return `/api/metrics/${metric}/custom-query.csv?${buildCustomQueryParams(
    metric,
    customFilters,
    availableSeasons,
    availableCustomTeams,
  ).toString()}`
}

export function buildCachedLeaderboardParams(
  metric: MetricId,
  cachedFilters: CachedFilters,
  filters: MetricFilters,
): URLSearchParams {
  const params = new URLSearchParams({
    top_n: String(cachedFilters.topN),
    min_average_minutes: String(filters.min_average_minutes),
    min_total_minutes: String(filters.min_total_minutes),
  })

  appendMetricSpecificParams(params, metric, filters)

  if (cachedFilters.teamId !== null) {
    params.set('team_id', String(cachedFilters.teamId))
  }

  return params
}

export function buildCustomQueryParams(
  metric: MetricId,
  customFilters: CustomFilters,
  availableSeasons: string[],
  availableCustomTeams: TeamOption[],
): URLSearchParams {
  const params = new URLSearchParams({
    top_n: String(customFilters.topN),
    min_average_minutes: String(customFilters.minAverageMinutes),
    min_total_minutes: String(customFilters.minTotalMinutes),
  })

  if (metric === 'rawr') {
    params.set('min_games', String(customFilters.minGames))
    params.set('ridge_alpha', String(customFilters.ridgeAlpha))
  } else {
    params.set('min_games_with', String(customFilters.minGamesWith))
    params.set('min_games_without', String(customFilters.minGamesWithout))
  }

  for (const teamId of filterSelectedTeamIdsForAvailableTeams(customFilters.teams, availableCustomTeams)) {
    params.append('team_id', String(teamId))
  }

  for (const season of seasonSpan(customFilters.startSeason, customFilters.endSeason, availableSeasons)) {
    params.append('season', season)
  }

  return params
}

export function sanitizeNumber(value: number): number {
  return Number.isFinite(value) ? value : 0
}

export function readNumberValue(rawValue: string): number {
  return sanitizeNumber(Number(rawValue))
}

function appendMetricSpecificParams(
  params: URLSearchParams,
  metric: MetricId,
  filters: MetricFilters,
): void {
  if (metric === 'rawr') {
    const rawrFilters = filters as RawrMetricFilters
    params.set('min_games', String(rawrFilters.min_games))
    params.set('ridge_alpha', String(rawrFilters.ridge_alpha))
    return
  }

  const wowyFilters = filters as WowyMetricFilters
  params.set('min_games_with', String(wowyFilters.min_games_with))
  params.set('min_games_without', String(wowyFilters.min_games_without))
}
