import type { TeamOption } from '../app/leaderboardApiTypes'
import type { LeaderboardFilters, LeaderboardNumberField } from '../app/leaderboardTypes'
import { NumericField } from './NumericField'
import { TeamSelector } from './TeamSelector'

type FilterFieldConfig = {
  key: LeaderboardNumberField
  label: string
  value: number
  step?: string
}

type LeaderboardFiltersPanelProps = {
  filters: LeaderboardFilters
  availableSeasons: string[]
  availableTeams: TeamOption[]
  isBootstrapping: boolean
  isLoading: boolean
  isRawrMetric: boolean
  onStartSeasonChange: (season: string) => void
  onEndSeasonChange: (season: string) => void
  onSelectAllTeams: () => void
  onToggleTeam: (teamId: number) => void
  onNumberChange: (field: LeaderboardNumberField, value: number) => void
  onRefresh: () => void
}

export function LeaderboardFiltersPanel({
  filters,
  availableSeasons,
  availableTeams,
  isBootstrapping,
  isLoading,
  isRawrMetric,
  onStartSeasonChange,
  onEndSeasonChange,
  onSelectAllTeams,
  onToggleTeam,
  onNumberChange,
  onRefresh,
}: LeaderboardFiltersPanelProps) {
  const isDisabled = isBootstrapping || isLoading
  const hasSelectedTeams = availableTeams.length > 0
  const metricFields: FilterFieldConfig[] = isRawrMetric
    ? [
      {
        key: 'minGames',
        label: 'Min games',
        value: filters.minGames,
      },
      {
        key: 'ridgeAlpha',
        label: 'Ridge alpha',
        value: filters.ridgeAlpha,
        step: '0.5',
      },
    ]
    : [
      {
        key: 'minGamesWith',
        label: 'Min games with',
        value: filters.minGamesWith,
      },
      {
        key: 'minGamesWithout',
        label: 'Min games without',
        value: filters.minGamesWithout,
      },
    ]

  return (
    <aside className="sidebar-panel">
      <div className="sidebar-panel__section">
        <div>
          <p className="panel-label">Leaderboard query</p>
        </div>
        <p className="sidebar-note">
          Select the seasons, teams, and thresholds you want. The backend serves cached rows when
          it can and computes live results when it has to.
        </p>
      </div>

      <div className="sidebar-panel__section">
        <p className="section-title">Scope</p>
        <div className="field-grid">
          <label className="field">
            <span>Start season</span>
            <select
              value={filters.startSeason}
              onChange={(event) => onStartSeasonChange(event.target.value)}
              disabled={isDisabled}
            >
              {availableSeasons.map((season) => (
                <option key={season} value={season}>
                  {season}
                </option>
              ))}
            </select>
          </label>

          <label className="field">
            <span>End season</span>
            <select
              value={filters.endSeason}
              onChange={(event) => onEndSeasonChange(event.target.value)}
              disabled={isDisabled}
            >
              {availableSeasons.map((season) => (
                <option key={season} value={season}>
                  {season}
                </option>
              ))}
            </select>
          </label>

          <NumericField
            label="Top players"
            min="1"
            max="100"
            value={filters.topN}
            disabled={isDisabled}
            onChange={(value) => onNumberChange('topN', value)}
          />

          <NumericField
            label="Min average minutes"
            min="0"
            step="0.5"
            value={filters.minAverageMinutes}
            disabled={isDisabled}
            onChange={(value) => onNumberChange('minAverageMinutes', value)}
          />

          <NumericField
            label="Min total minutes"
            min="0"
            step="10"
            value={filters.minTotalMinutes}
            disabled={isDisabled}
            onChange={(value) => onNumberChange('minTotalMinutes', value)}
          />

          {metricFields.map((field) => (
            <NumericField
              key={field.key}
              label={field.label}
              min="0"
              step={field.step}
              value={field.value}
              disabled={isDisabled}
              onChange={(value) => onNumberChange(field.key, value)}
            />
          ))}
        </div>
      </div>

      <fieldset className="sidebar-panel__section team-fieldset">
        <legend className="section-title">Teams</legend>
        <TeamSelector
          availableTeams={availableTeams}
          selectedTeamIds={filters.teamIds}
          disabled={isDisabled}
          onSelectAll={onSelectAllTeams}
          onToggleTeam={onToggleTeam}
        />

        {!hasSelectedTeams ? (
          <p className="sidebar-note">No teams are available for the current season span.</p>
        ) : null}
      </fieldset>

      <div className="sidebar-panel__footer">
        <button
          type="button"
          className="primary-button"
          onClick={onRefresh}
          disabled={isDisabled || !filters.startSeason || !filters.endSeason || !hasSelectedTeams}
        >
          {isLoading ? 'Refreshing...' : 'Refresh leaderboard'}
        </button>
      </div>
    </aside>
  )
}
