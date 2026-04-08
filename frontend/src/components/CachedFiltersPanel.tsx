import type { CachedFilters, MetricFilters, MetricNumberField, TeamOption } from '../app/types'
import { NumericField } from './NumericField'

type MetricFieldConfig = {
  key: MetricNumberField
  label: string
  value: number
  step?: string
}

type CachedFiltersPanelProps = {
  cachedFilters: CachedFilters
  metricFilters: MetricFilters
  availableTeams: TeamOption[]
  isBootstrapping: boolean
  isLoading: boolean
  isRawrMetric: boolean
  onTeamChange: (teamId: number | null) => void
  onTopNChange: (value: number) => void
  onMetricFilterChange: (field: MetricNumberField, value: number) => void
  onRefresh: () => void
}

export function CachedFiltersPanel({
  cachedFilters,
  metricFilters,
  availableTeams,
  isBootstrapping,
  isLoading,
  isRawrMetric,
  onTeamChange,
  onTopNChange,
  onMetricFilterChange,
  onRefresh,
}: CachedFiltersPanelProps) {
  const isDisabled = isBootstrapping || isLoading
  const metricSpecificFields: MetricFieldConfig[] = isRawrMetric
    ? [
        {
          key: 'minGames',
          label: 'Min games',
          value: 'min_games' in metricFilters ? metricFilters.min_games : 0,
        },
        {
          key: 'ridgeAlpha',
          label: 'Ridge alpha',
          value: 'ridge_alpha' in metricFilters ? metricFilters.ridge_alpha : 0,
          step: '0.5',
        },
      ]
    : [
        {
          key: 'minGamesWith',
          label: 'Min games with',
          value: 'min_games_with' in metricFilters ? metricFilters.min_games_with : 0,
        },
        {
          key: 'minGamesWithout',
          label: 'Min games without',
          value: 'min_games_without' in metricFilters ? metricFilters.min_games_without : 0,
        },
      ]

  return (
    <aside className="sidebar-panel">
      <div className="sidebar-panel__section">
        <div>
          <p className="panel-label">Cached leaderboard</p>
          <h2>Prebuilt leaderboard filters</h2>
        </div>
        <p className="sidebar-note">
          Filter the stored leaderboard before ranking the current span.
        </p>
      </div>

      <div className="sidebar-panel__section">
        <div className="field-grid">
          <label className="field">
            <span>Team scope</span>
            <select
              value={cachedFilters.teamId ?? ''}
              onChange={(event) =>
                onTeamChange(event.target.value ? Number(event.target.value) : null)
              }
              disabled={isDisabled}
            >
              <option value="">All teams</option>
              {availableTeams.map((team) => (
                <option key={team.team_id} value={team.team_id}>
                  {team.label}
                </option>
              ))}
            </select>
          </label>

          <NumericField
            label="Top players"
            min="1"
            max="100"
            value={cachedFilters.topN}
            disabled={isDisabled}
            onChange={onTopNChange}
          />

          <NumericField
            label="Min average minutes"
            min="0"
            step="0.5"
            value={metricFilters.min_average_minutes}
            disabled={isDisabled}
            onChange={(value) => onMetricFilterChange('minAverageMinutes', value)}
          />

          <NumericField
            label="Min total minutes"
            min="0"
            step="10"
            value={metricFilters.min_total_minutes}
            disabled={isDisabled}
            onChange={(value) => onMetricFilterChange('minTotalMinutes', value)}
          />

          {metricSpecificFields.map((field) => (
            <NumericField
              key={field.key}
              label={field.label}
              min="0"
              step={field.step}
              value={field.value}
              disabled={isDisabled}
              onChange={(value) => onMetricFilterChange(field.key, value)}
            />
          ))}
        </div>
      </div>

      <div className="sidebar-panel__footer">
        <button type="button" className="primary-button" onClick={onRefresh} disabled={isDisabled}>
          {isLoading ? 'Refreshing...' : 'Refresh leaderboard'}
        </button>
      </div>
    </aside>
  )
}
