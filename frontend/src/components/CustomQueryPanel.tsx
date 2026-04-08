import type { CustomFilters, CustomNumberField, TeamOption } from '../app/types'
import { NumericField } from './NumericField'

type CustomFieldConfig = {
  key: CustomNumberField
  label: string
  value: number
  step?: string
}

type CustomQueryPanelProps = {
  customFilters: CustomFilters
  availableSeasons: string[]
  availableTeams: TeamOption[]
  isBootstrapping: boolean
  isLoading: boolean
  isRawrMetric: boolean
  allTeamsSelected: boolean
  onStartSeasonChange: (season: string) => void
  onEndSeasonChange: (season: string) => void
  onToggleAllTeams: () => void
  onToggleTeam: (teamId: number) => void
  onNumberChange: (field: CustomNumberField, value: number) => void
  onRunQuery: () => void
}

export function CustomQueryPanel({
  customFilters,
  availableSeasons,
  availableTeams,
  isBootstrapping,
  isLoading,
  isRawrMetric,
  allTeamsSelected,
  onStartSeasonChange,
  onEndSeasonChange,
  onToggleAllTeams,
  onToggleTeam,
  onNumberChange,
  onRunQuery,
}: CustomQueryPanelProps) {
  const hasSelectedTeams = customFilters.teams.length > 0
  const queryFields: CustomFieldConfig[] = isRawrMetric
    ? [
        {
          key: 'minGames',
          label: 'Min games',
          value: customFilters.minGames,
        },
        {
          key: 'ridgeAlpha',
          label: 'Ridge alpha',
          value: customFilters.ridgeAlpha,
          step: '0.5',
        },
      ]
    : [
        {
          key: 'minGamesWith',
          label: 'Min games with',
          value: customFilters.minGamesWith,
        },
        {
          key: 'minGamesWithout',
          label: 'Min games without',
          value: customFilters.minGamesWithout,
        },
      ]

  return (
    <aside className="sidebar-panel">
      <div className="sidebar-panel__section">
        <div>
          <p className="panel-label">Custom query</p>
          <h2>Live slice builder</h2>
        </div>
        <p className="sidebar-note">
          Choose a season span, restrict the team pool, and rerun the metric on demand.
        </p>
      </div>

      <div className="sidebar-panel__section">
        <p className="section-title">Scope</p>
        <div className="field-grid">
          <label className="field">
            <span>Start season</span>
            <select
              value={customFilters.startSeason}
              onChange={(event) => onStartSeasonChange(event.target.value)}
              disabled={isBootstrapping || isLoading}
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
              value={customFilters.endSeason}
              onChange={(event) => onEndSeasonChange(event.target.value)}
              disabled={isBootstrapping || isLoading}
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
            value={customFilters.topN}
            disabled={isBootstrapping || isLoading}
            onChange={(value) => onNumberChange('topN', value)}
          />
        </div>
      </div>

      <div className="sidebar-panel__section">
        <p className="section-title">{isRawrMetric ? 'Model filters' : 'Query filters'}</p>
        <div className="field-grid">
          {queryFields.map((field) => (
            <NumericField
              key={field.key}
              label={field.label}
              min="0"
              step={field.step}
              value={field.value}
              disabled={isBootstrapping || isLoading}
              onChange={(value) => onNumberChange(field.key, value)}
            />
          ))}

          <NumericField
            label="Min average minutes"
            min="0"
            step="0.5"
            value={customFilters.minAverageMinutes}
            disabled={isBootstrapping || isLoading}
            onChange={(value) => onNumberChange('minAverageMinutes', value)}
          />

          <NumericField
            label="Min total minutes"
            min="0"
            step="10"
            value={customFilters.minTotalMinutes}
            disabled={isBootstrapping || isLoading}
            onChange={(value) => onNumberChange('minTotalMinutes', value)}
          />
        </div>
      </div>

      <fieldset className="sidebar-panel__section team-fieldset">
        <legend className="section-title">Teams</legend>
        <div className="team-grid">
          <button
            type="button"
            className={allTeamsSelected ? 'team-toggle is-selected' : 'team-toggle'}
            onClick={onToggleAllTeams}
            disabled={isBootstrapping || isLoading || availableTeams.length === 0}
          >
            All
          </button>

          {availableTeams.map((team) => {
            const isSelected = customFilters.teams.includes(team.team_id)
            return (
              <label key={team.team_id} className={isSelected ? 'team-chip is-selected' : 'team-chip'}>
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={() => onToggleTeam(team.team_id)}
                  disabled={isBootstrapping || isLoading}
                />
                <span>{team.label}</span>
              </label>
            )
          })}
        </div>

        {!hasSelectedTeams ? (
          <p className="sidebar-note">Select at least one team to run a custom query.</p>
        ) : null}
      </fieldset>

      <div className="sidebar-panel__footer">
        <button
          type="button"
          className="primary-button"
          onClick={onRunQuery}
          disabled={
            isBootstrapping ||
            isLoading ||
            !customFilters.startSeason ||
            !customFilters.endSeason ||
            !hasSelectedTeams
          }
        >
          {isLoading ? 'Running...' : 'Run query'}
        </button>
      </div>
    </aside>
  )
}
