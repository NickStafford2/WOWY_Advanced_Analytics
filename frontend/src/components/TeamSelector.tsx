import { isAllTeamsSelection } from '../app/query'
import type { TeamOption } from '../app/types'

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

  return (
    <div className="team-grid">
      <button
        type="button"
        className={allTeamsSelected ? 'team-toggle is-selected' : 'team-toggle'}
        onClick={onSelectAll}
        disabled={disabled || availableTeams.length === 0}
      >
        All
      </button>

      {availableTeams.map((team) => {
        const isSelected = allTeamsSelected || selectedTeamIds.includes(team.team_id)
        return (
          <label key={team.team_id} className={isSelected ? 'team-chip is-selected' : 'team-chip'}>
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
  )
}
