import { isAllTeamsSelection } from '../app/leaderboardQuery'
import type { TeamOption } from '../app/leaderboardApiTypes'

const SELECTED_CLASS_NAME =
  'border-[color:var(--accent-border)] [background:var(--accent-gradient-strong)] text-[color:var(--text-inverse)]'
const UNSELECTED_BUTTON_CLASS_NAME =
  'border-[color:var(--control-border)] bg-[var(--input-background)] text-[color:var(--text-secondary)]'
const UNSELECTED_CHIP_CLASS_NAME =
  'border-[color:var(--control-border)] bg-[var(--chip-background)] text-[color:var(--text-secondary)]'

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
    <div className="flex flex-col gap-[10px]">
      <div className="flex">
        <button
          type="button"
          className={`flex min-h-9 cursor-pointer items-center justify-center rounded-xl border px-[18px] text-center font-bold ${allTeamsSelected ? SELECTED_CLASS_NAME : UNSELECTED_BUTTON_CLASS_NAME} disabled:cursor-not-allowed disabled:opacity-60`}
          onClick={onSelectAll}
          disabled={disabled || !hasAvailableTeams}
        >
          All teams
        </button>
      </div>

      {hasAvailableTeams ? (
        <div className="grid grid-cols-[repeat(auto-fit,minmax(46px,1fr))] gap-1">
          {availableTeams.map((team) => {
            const isSelected = allTeamsSelected || selectedTeamIds.includes(team.team_id)
            return (
              <label
                key={team.team_id}
                className={`relative flex min-h-6 items-center justify-center rounded-xl border px-[10px] text-center ${isSelected ? SELECTED_CLASS_NAME : UNSELECTED_CHIP_CLASS_NAME} ${disabled ? 'cursor-not-allowed opacity-60' : 'cursor-pointer'}`}
              >
                <input
                  className="pointer-events-none absolute opacity-0"
                  type="checkbox"
                  checked={isSelected}
                  onChange={() => onToggleTeam(team.team_id)}
                  disabled={disabled}
                />
                <span className="text-[0.76rem] font-bold">{team.label}</span>
              </label>
            )
          })}
        </div>
      ) : null}
    </div>
  )
}
