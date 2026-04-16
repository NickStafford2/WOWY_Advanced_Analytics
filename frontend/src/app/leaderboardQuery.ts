import type { MetricOptionsPayload, TeamOption } from './leaderboardApiTypes'
import type {
  LeaderboardFilters,
  LeaderboardNumberField,
  LeaderboardSeasonType,
} from './leaderboardTypes'
import type { MetricId } from './metricTypes'

export const DEFAULT_SEASON_TYPES: LeaderboardSeasonType[] = ['REGULAR', 'PLAYOFFS']
const SEASON_TYPE_ORDER: LeaderboardSeasonType[] = ['REGULAR', 'PLAYOFFS', 'PRESEASON']

export function defaultLeaderboardFilters(): LeaderboardFilters {
  return {
    startSeason: '',
    endSeason: '',
    seasonTypes: [...DEFAULT_SEASON_TYPES],
    teamIds: null,
    topN: 100,
    minGames: 35,
    ridgeAlpha: 10,
    minGamesWith: 15,
    minGamesWithout: 2,
    minAverageMinutes: 30,
    minTotalMinutes: 600,
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

export function buildMetricOptionsParamsForTeams(selectedTeamIds: number[] | null): URLSearchParams {
  const params = new URLSearchParams()
  if (selectedTeamIds === null) {
    return params
  }

  for (const teamId of selectedTeamIds) {
    params.append('team_id', String(teamId))
  }

  return params
}

export function buildMetricOptionsParamsForSeasonSpan(
  filters: LeaderboardFilters,
  availableSeasons: string[],
): URLSearchParams {
  const params = new URLSearchParams()
  const selectedSeasons = seasonSpan(filters.startSeason, filters.endSeason, availableSeasons)
  for (const season of selectedSeasons) {
    params.append('season', `${season}:REGULAR`)
  }

  return params
}

export function filterSelectedTeamIdsForAvailableTeams(
  selectedTeamIds: number[] | null,
  availableTeams: TeamOption[],
): number[] {
  if (selectedTeamIds === null) {
    return availableTeams.map((team) => team.team_id)
  }

  const availableTeamIds = new Set(availableTeams.map((team) => team.team_id))
  return selectedTeamIds.filter((teamId) => availableTeamIds.has(teamId))
}

export function isAllTeamsSelection(
  selectedTeamIds: number[] | null,
  availableTeams: TeamOption[],
): boolean {
  if (availableTeams.length === 0) {
    return false
  }

  return _normalizeSelectedTeamIds(selectedTeamIds, availableTeams) === null
}

export function toggleSelectedTeam(
  selectedTeamIds: number[] | null,
  teamId: number,
  availableTeams: TeamOption[],
): number[] | null {
  const normalizedTeamIds = _normalizeSelectedTeamIds(selectedTeamIds, availableTeams)

  if (normalizedTeamIds === null) {
    return availableTeams
      .map((team) => team.team_id)
      .filter((availableTeamId) => availableTeamId !== teamId)
  }

  if (normalizedTeamIds.includes(teamId)) {
    return _normalizeSelectedTeamIds(
      normalizedTeamIds.filter((selectedTeamId) => selectedTeamId !== teamId),
      availableTeams,
    )
  }

  return _normalizeSelectedTeamIds([...normalizedTeamIds, teamId], availableTeams)
}

export function selectAllTeams(): null {
  return null
}

export function buildExportUrl({
  metric,
  filters,
  availableSeasons,
  availableTeams,
}: {
  metric: MetricId
  filters: LeaderboardFilters
  availableSeasons: string[]
  availableTeams: TeamOption[]
}): string {
  return `/api/metrics/${metric}/leaderboard.csv?${buildLeaderboardParams(
    metric,
    filters,
    availableSeasons,
    availableTeams,
  ).toString()}`
}

export function buildLeaderboardParams(
  metric: MetricId,
  filters: LeaderboardFilters,
  availableSeasons: string[],
  availableTeams: TeamOption[],
): URLSearchParams {
  const selectedSeasonSpan = seasonSpan(filters.startSeason, filters.endSeason, availableSeasons)
  const selectedSeasonIds = buildSelectedSeasonIds(selectedSeasonSpan, filters.seasonTypes)
  const selectedTeamIds = filterSelectedTeamIdsForAvailableTeams(filters.teamIds, availableTeams)
  const params = new URLSearchParams({
    top_n: String(filters.topN),
    min_average_minutes: String(filters.minAverageMinutes),
    min_total_minutes: String(filters.minTotalMinutes),
  })

  if (metric === 'rawr') {
    params.set('min_games', String(filters.minGames))
    params.set('ridge_alpha', String(filters.ridgeAlpha))
  } else {
    params.set('min_games_with', String(filters.minGamesWith))
    params.set('min_games_without', String(filters.minGamesWithout))
  }

  const isAllTeamsSelected = isAllTeamsSelection(filters.teamIds, availableTeams)

  if (!isAllTeamsSelected) {
    for (const teamId of selectedTeamIds) {
      params.append('team_id', String(teamId))
    }
  }

  for (const seasonId of selectedSeasonIds) {
    params.append('season', seasonId)
  }

  return params
}

export function buildSelectedSeasonIds(
  seasons: string[],
  seasonTypes: LeaderboardSeasonType[],
): string[] {
  const selectedSeasonTypes = seasonTypes.length > 0 ? seasonTypes : DEFAULT_SEASON_TYPES
  const seasonIds: string[] = []
  for (const season of seasons) {
    for (const seasonType of selectedSeasonTypes) {
      seasonIds.push(`${season}:${seasonType}`)
    }
  }
  return seasonIds
}

export function toggleLeaderboardSeasonType(
  selectedSeasonTypes: LeaderboardSeasonType[],
  seasonType: LeaderboardSeasonType,
): LeaderboardSeasonType[] {
  if (!selectedSeasonTypes.includes(seasonType)) {
    return _orderSeasonTypes([...selectedSeasonTypes, seasonType])
  }
  if (selectedSeasonTypes.length === 1) {
    return selectedSeasonTypes
  }
  return selectedSeasonTypes.filter((selectedSeasonType) => selectedSeasonType !== seasonType)
}

function _orderSeasonTypes(seasonTypes: LeaderboardSeasonType[]): LeaderboardSeasonType[] {
  return SEASON_TYPE_ORDER.filter((seasonType) => seasonTypes.includes(seasonType))
}

export function updateLeaderboardFilterValue(
  filters: LeaderboardFilters,
  field: LeaderboardNumberField,
  value: number,
): LeaderboardFilters {
  return {
    ...filters,
    [field]: sanitizeNumber(value),
  }
}

export function sanitizeNumber(value: number): number {
  return Number.isFinite(value) ? value : 0
}

export function readNumberValue(rawValue: string): number {
  return sanitizeNumber(Number(rawValue))
}

export function initializeLeaderboardFiltersWithOptions(
  current: LeaderboardFilters,
  payload: MetricOptionsPayload,
): LeaderboardFilters {
  const scopeFilters = syncScopedLeaderboardFiltersWithOptions(current, payload)
  return {
    ...scopeFilters,
    topN: payload.filters.top_n,
    minGames: payload.metric === 'rawr' ? payload.filters.min_games : current.minGames,
    ridgeAlpha: payload.metric === 'rawr' ? payload.filters.ridge_alpha : current.ridgeAlpha,
    minGamesWith: payload.metric === 'rawr' ? current.minGamesWith : payload.filters.min_games_with,
    minGamesWithout:
      payload.metric === 'rawr' ? current.minGamesWithout : payload.filters.min_games_without,
    minAverageMinutes: payload.filters.min_average_minutes,
    minTotalMinutes: payload.filters.min_total_minutes,
  }
}

export function syncScopedLeaderboardFiltersWithOptions(
  filters: LeaderboardFilters,
  payload: MetricOptionsPayload,
): LeaderboardFilters {
  const defaultStartSeason = payload.available_seasons[0] || ''
  const defaultEndSeason = payload.available_seasons[payload.available_seasons.length - 1] || ''
  const nextStartSeason = payload.available_seasons.includes(filters.startSeason)
    ? filters.startSeason
    : defaultStartSeason
  const nextEndSeason = payload.available_seasons.includes(filters.endSeason)
    ? filters.endSeason
    : defaultEndSeason
  const nextTeamIds = _normalizeSelectedTeamIds(filters.teamIds, payload.team_options)

  if (
    nextStartSeason === filters.startSeason &&
    nextEndSeason === filters.endSeason &&
    _teamIdsEqual(nextTeamIds, filters.teamIds)
  ) {
    return filters
  }

  return {
    ...filters,
    startSeason: nextStartSeason,
    endSeason: nextEndSeason,
    teamIds: nextTeamIds,
  }
}

function _normalizeSelectedTeamIds(
  selectedTeamIds: number[] | null,
  availableTeams: TeamOption[],
): number[] | null {
  if (selectedTeamIds === null) {
    return availableTeams.length === 0 ? [] : null
  }

  const availableTeamIds = new Set(availableTeams.map((team) => team.team_id))
  const nextTeamIds = selectedTeamIds.filter((teamId) => availableTeamIds.has(teamId))
  if (nextTeamIds.length === 0) {
    return []
  }
  if (nextTeamIds.length === availableTeams.length) {
    return null
  }

  return nextTeamIds
}

function _teamIdsEqual(left: number[] | null, right: number[] | null): boolean {
  if (left === right) {
    return true
  }
  if (left === null || right === null) {
    return false
  }
  if (left.length !== right.length) {
    return false
  }

  return left.every((teamId, index) => teamId === right[index])
}
