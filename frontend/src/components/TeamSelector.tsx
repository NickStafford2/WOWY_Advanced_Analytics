import { useEffect, useState } from 'react'
import { isAllTeamsSelection, toggleSelectedTeam } from '../app/leaderboardQuery'
import type { TeamOption } from '../app/leaderboardApiTypes'

type TeamSelectorProps = {
  availableTeams: TeamOption[]
  selectedTeamIds: number[]
  disabled: boolean
  onSelectAll: () => void
  onToggleTeam: (teamId: number) => void
}

export function TeamSelector({
  availableTeams,
  selectedTeamIds,
  disabled,
  onSelectAll,
  onToggleTeam,
}: TeamSelectorProps) {
  const allTeamsSelected = isAllTeamsSelection(selectedTeamIds, availableTeams)
  const [isCustomMode, setIsCustomMode] = useState(selectedTeamIds.length > 0)

  useEffect(() => {
    if (selectedTeamIds.length > 0) {
      setIsCustomMode(true)
      return
    }

    if (availableTeams.length === 0) {
      setIsCustomMode(false)
    }
  }, [availableTeams.length, selectedTeamIds.length])

  const isCustomSelected = isCustomMode || !allTeamsSelected
  const isAllSelected = !isCustomSelected

  function handleSelectCustomMode(): void {
    setIsCustomMode(true)
  }

  function handleSelectAllMode(): void {
    setIsCustomMode(false)
    onSelectAll()
  }

  function handleToggleTeam(teamId: number): void {
    const nextSelectedTeamIds = toggleSelectedTeam(selectedTeamIds, teamId, availableTeams)
    if (isAllTeamsSelection(nextSelectedTeamIds, availableTeams)) {
      setIsCustomMode(false)
    } else {
      setIsCustomMode(true)
    }

    onToggleTeam(teamId)
  }

  return (
    <div className="team-selector">
      <div className="team-selector__mode-row">
        <button
          type="button"
          className={isAllSelected ? 'team-toggle is-selected' : 'team-toggle'}
          onClick={handleSelectAllMode}
          disabled={disabled || availableTeams.length === 0}
        >
          All
        </button>

        <button
          type="button"
          className={isCustomSelected ? 'team-toggle is-selected' : 'team-toggle'}
          onClick={handleSelectCustomMode}
          disabled={disabled || availableTeams.length === 0}
        >
          Custom
        </button>
      </div>

      {isCustomSelected ? (
        <div className="team-grid">
          {availableTeams.map((team) => {
            const isSelected = allTeamsSelected || selectedTeamIds.includes(team.team_id)
            return (
              <label
                key={team.team_id}
                className={isSelected ? 'team-chip is-selected' : 'team-chip'}
              >
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={() => handleToggleTeam(team.team_id)}
                  disabled={disabled}
                />
                <span>{team.label}</span>
              </label>
            )
          })}
        </div>
      ) : null}
    </div>
  )
}
