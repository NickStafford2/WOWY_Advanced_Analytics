import { updateLeaderboardFilterValue } from './leaderboardFilters'
import { toggleLeaderboardSeasonType } from './leaderboardSeason'
import {
  isAllTeamsSelection,
  selectAllTeams,
  toggleSelectedTeam,
} from './leaderboardTeams'
import type { TeamOption } from './leaderboardApiTypes'
import type {
  LeaderboardFilters,
  LeaderboardNumberField,
  LeaderboardSeasonType,
} from './leaderboardTypes'

type ApplyScopedFilterChange = (
  buildNextFilters: (current: LeaderboardFilters) => LeaderboardFilters,
) => void

type UseLeaderboardControlsValue = {
  setStartSeason: (season: string) => void
  setEndSeason: (season: string) => void
  toggleSeasonType: (seasonType: LeaderboardSeasonType) => void
  selectAllTeams: () => void
  toggleTeam: (teamId: number) => void
  setNumberFilter: (field: LeaderboardNumberField, value: number) => void
}

export function useLeaderboardControls({
  filters,
  availableTeams,
  setFilters,
  applyScopedFilterChange,
}: {
  filters: LeaderboardFilters
  availableTeams: TeamOption[]
  setFilters: (next: LeaderboardFilters | ((current: LeaderboardFilters) => LeaderboardFilters)) => void
  applyScopedFilterChange: ApplyScopedFilterChange
}): UseLeaderboardControlsValue {
  function setStartSeason(season: string): void {
    applyScopedFilterChange((current) => ({ ...current, startSeason: season }))
  }

  function setEndSeason(season: string): void {
    applyScopedFilterChange((current) => ({ ...current, endSeason: season }))
  }

  function toggleSeasonType(seasonType: LeaderboardSeasonType): void {
    setFilters({
      ...filters,
      seasonTypes: toggleLeaderboardSeasonType(filters.seasonTypes, seasonType),
    })
  }

  function handleSelectAllTeams(): void {
    applyScopedFilterChange((current) => ({
      ...current,
      teamIds: isAllTeamsSelection(current.teamIds, availableTeams) ? [] : selectAllTeams(),
    }))
  }

  function handleToggleTeam(teamId: number): void {
    applyScopedFilterChange((current) => ({
      ...current,
      teamIds: toggleSelectedTeam(current.teamIds, teamId, availableTeams),
    }))
  }

  function setNumberFilter(field: LeaderboardNumberField, value: number): void {
    setFilters((current) => updateLeaderboardFilterValue(current, field, value))
  }

  return {
    setStartSeason,
    setEndSeason,
    toggleSeasonType,
    selectAllTeams: handleSelectAllTeams,
    toggleTeam: handleToggleTeam,
    setNumberFilter,
  }
}
