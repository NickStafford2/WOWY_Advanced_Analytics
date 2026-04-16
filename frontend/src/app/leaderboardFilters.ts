import type { MetricOptionsPayload } from './leaderboardApiTypes'
import type { LeaderboardFilters, LeaderboardNumberField } from './leaderboardTypes'
import { DEFAULT_SEASON_TYPES } from './leaderboardSeason'
import { normalizeSelectedTeamIds, teamIdsEqual } from './leaderboardTeams'

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
  const nextTeamIds = normalizeSelectedTeamIds(filters.teamIds, payload.team_options)

  if (
    nextStartSeason === filters.startSeason &&
    nextEndSeason === filters.endSeason &&
    teamIdsEqual(nextTeamIds, filters.teamIds)
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
