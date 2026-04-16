import type { TeamOption } from './leaderboardApiTypes'

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

export function normalizeSelectedTeamIds(
  selectedTeamIds: number[] | null,
  availableTeams: TeamOption[],
): number[] | null {
  return _normalizeSelectedTeamIds(selectedTeamIds, availableTeams)
}

export function teamIdsEqual(left: number[] | null, right: number[] | null): boolean {
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
