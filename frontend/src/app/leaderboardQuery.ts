import type { MetricOptionsPayload, TeamOption } from './leaderboardApiTypes'
import type { LeaderboardFilters, LeaderboardNumberField } from './leaderboardTypes'
import type { MetricId } from './metricTypes'

export function defaultLeaderboardFilters(): LeaderboardFilters {
  return {
    startSeason: '',
    endSeason: '',
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

export function syncLeaderboardFiltersWithOptions(
  current: LeaderboardFilters,
  payload: MetricOptionsPayload,
): LeaderboardFilters {
  const defaultStartSeason = payload.available_seasons[0] || ''
  const defaultEndSeason = payload.available_seasons[payload.available_seasons.length - 1] || ''
  const selectedTeamIds = _normalizeSelectedTeamIds(current.teamIds, payload.team_options)

  return {
    ...current,
    startSeason: payload.available_seasons.includes(current.startSeason)
      ? current.startSeason
      : defaultStartSeason,
    endSeason: payload.available_seasons.includes(current.endSeason)
      ? current.endSeason
      : defaultEndSeason,
    teamIds: selectedTeamIds,
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
    seasonsInScope.some((season) => teamOption.available_seasons.includes(season)),
  )
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

export function syncSelectedTeamIds(
  selectedTeamIds: number[] | null,
  availableTeams: TeamOption[],
): number[] | null {
  return _normalizeSelectedTeamIds(selectedTeamIds, availableTeams)
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

  const isFullSeasonSpan =
    selectedSeasonSpan.length === availableSeasons.length &&
    selectedSeasonSpan.every((season, index) => season === availableSeasons[index])
  const isAllTeamsSelected = isAllTeamsSelection(filters.teamIds, availableTeams)

  if (!isAllTeamsSelected) {
    for (const teamId of selectedTeamIds) {
      params.append('team_id', String(teamId))
    }
  }

  if (!isFullSeasonSpan) {
    for (const season of selectedSeasonSpan) {
      params.append('season', season)
    }
  }

  return params
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
