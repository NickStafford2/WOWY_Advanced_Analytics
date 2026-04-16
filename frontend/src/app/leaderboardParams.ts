import type { TeamOption } from './leaderboardApiTypes'
import type { LeaderboardFilters } from './leaderboardTypes'
import type { MetricId } from './metricTypes'
import { buildSelectedSeasonIds, seasonSpan } from './leaderboardSeason'
import { filterSelectedTeamIdsForAvailableTeams, isAllTeamsSelection } from './leaderboardTeams'

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

  if (!isAllTeamsSelection(filters.teamIds, availableTeams)) {
    for (const teamId of selectedTeamIds) {
      params.append('team_id', String(teamId))
    }
  }

  for (const seasonId of selectedSeasonIds) {
    params.append('season', seasonId)
  }

  return params
}
