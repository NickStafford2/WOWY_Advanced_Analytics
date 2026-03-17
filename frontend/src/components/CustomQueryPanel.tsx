import type { ChangeEvent } from 'react'

export type CustomFilters = {
  startSeason: string
  endSeason: string
  teams: string[]
  topN: number
  minGames: number
  ridgeAlpha: number
  minGamesWith: number
  minGamesWithout: number
  minAverageMinutes: number
  minTotalMinutes: number
}

export type CustomNumberField =
  | 'topN'
  | 'minGames'
  | 'ridgeAlpha'
  | 'minGamesWith'
  | 'minGamesWithout'
  | 'minAverageMinutes'
  | 'minTotalMinutes'

type CustomQueryPanelProps = {
  customFilters: CustomFilters
  availableSeasons: string[]
  availableTeams: string[]
  isBootstrapping: boolean
  isLoading: boolean
  isRawrMetric: boolean
  allTeamsSelected: boolean
  onStartSeasonChange: (season: string) => void
  onEndSeasonChange: (season: string) => void
  onToggleAllTeams: () => void
  onToggleTeam: (team: string) => void
  onNumberChange: (field: CustomNumberField, event: ChangeEvent<HTMLInputElement>) => void
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

  return (
    <section className="query-panel">
      <div className="query-card query-card--span-2 query-card--scope">
        <div className="query-card-header">
          <p className="panel-label">Scope</p>
        </div>
        <div className="query-compact-grid">
          <label>
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

          <label>
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

          <label>
            <span>Top players</span>
            <input
              type="number"
              min="1"
              max="50"
              value={customFilters.topN}
              onChange={(event) => onNumberChange('topN', event)}
            />
          </label>
        </div>
      </div>

      <div className="query-card query-card--span-2 query-card--filters">
        <div className="query-card-header">
          <p className="panel-label">{isRawrMetric ? 'Model filters' : 'Query filters'}</p>
          <p className="query-section-note">
            {isRawrMetric
              ? 'Model entry and output thresholds are combined here to keep the query compact.'
              : 'Set minimum on and off samples plus minute thresholds for the result set.'}
          </p>
        </div>
        <div className="query-compact-grid">
          <label>
            <span>{isRawrMetric ? 'Min games' : 'Min games with'}</span>
            <input
              type="number"
              min="0"
              value={isRawrMetric ? customFilters.minGames : customFilters.minGamesWith}
              onChange={(event) =>
                onNumberChange(isRawrMetric ? 'minGames' : 'minGamesWith', event)
              }
            />
          </label>

          {isRawrMetric ? (
            <label>
              <span>Ridge alpha</span>
              <input
                type="number"
                min="0"
                step="0.5"
                value={customFilters.ridgeAlpha}
                onChange={(event) => onNumberChange('ridgeAlpha', event)}
              />
            </label>
          ) : (
            <label>
              <span>Min games without</span>
              <input
                type="number"
                min="0"
                value={customFilters.minGamesWithout}
                onChange={(event) => onNumberChange('minGamesWithout', event)}
              />
            </label>
          )}

          <label>
            <span>Min average minutes</span>
            <input
              type="number"
              min="0"
              step="0.5"
              value={customFilters.minAverageMinutes}
              onChange={(event) => onNumberChange('minAverageMinutes', event)}
            />
          </label>

          <label>
            <span>Min total minutes</span>
            <input
              type="number"
              min="0"
              step="10"
              value={customFilters.minTotalMinutes}
              onChange={(event) => onNumberChange('minTotalMinutes', event)}
            />
          </label>
        </div>
        <div className="query-actions">
          <button
            type="button"
            className="run-button query-run"
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
        {!hasSelectedTeams ? (
          <p className="query-section-note">Select at least one team to run a custom query.</p>
        ) : null}
      </div>

      <fieldset className="query-card query-card--span-2 query-card--teams query-multi">
        <legend>Teams</legend>
        <button
          type="button"
          className={allTeamsSelected ? 'team-toggle-button is-selected' : 'team-toggle-button'}
          onClick={onToggleAllTeams}
          disabled={isBootstrapping || isLoading || availableTeams.length === 0}
        >
          All
        </button>
        <div className="query-team-grid">
          {availableTeams.map((team) => {
            const isSelected = customFilters.teams.includes(team)
            return (
              <label key={team} className={isSelected ? 'team-chip is-selected' : 'team-chip'}>
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={() => onToggleTeam(team)}
                  disabled={isBootstrapping || isLoading}
                />
                <span>{team}</span>
              </label>
            )
          })}
        </div>
      </fieldset>
    </section>
  )
}
