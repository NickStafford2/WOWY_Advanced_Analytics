import { isAllTeamsSelection } from '../app/leaderboardQuery'
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
  const hasAvailableTeams = availableTeams.length > 0

  return (
    <div className="team-selector">
      <div className="team-selector__actions">
        <button
          type="button"
          className={allTeamsSelected ? 'team-toggle is-selected' : 'team-toggle'}
          onClick={onSelectAll}
          disabled={disabled || !hasAvailableTeams}
        >
          All teams
        </button>
      </div>

      {hasAvailableTeams ? (
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
                  onChange={() => onToggleTeam(team.team_id)}
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
