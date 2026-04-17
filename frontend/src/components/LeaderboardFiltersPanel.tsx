import type { TeamOption } from '../app/leaderboardApiTypes'
import type {
  LeaderboardFilters,
  LeaderboardNumberField,
  LeaderboardSeasonType,
} from '../app/leaderboardTypes'
import { NumericField } from './NumericField'
import { TeamSelector } from './TeamSelector'

const PANEL_LABEL_CLASS_NAME =
  'm-0 text-xs font-bold tracking-[0.16em] uppercase text-[color:var(--accent-warm)]'
const SECTION_TITLE_CLASS_NAME = 'm-0 text-[0.95rem] font-bold text-[color:var(--text-secondary)]'

type FilterFieldConfig = {
  key: LeaderboardNumberField
  label: string
  value: number
  step?: string
}

type SeasonTypeButtonConfig = {
  key: LeaderboardSeasonType
  label: string
}

const SEASON_TYPE_BUTTONS: SeasonTypeButtonConfig[] = [
  { key: 'REGULAR', label: 'Regular season' },
  { key: 'PLAYOFFS', label: 'Playoffs' },
  { key: 'PRESEASON', label: 'Preseason' },
]

type LeaderboardFiltersPanelProps = {
  filters: LeaderboardFilters
  availableSeasons: string[]
  availableTeams: TeamOption[]
  isBootstrapping: boolean
  isLoading: boolean
  isRawrMetric: boolean
  onStartSeasonChange: (season: string) => void
  onEndSeasonChange: (season: string) => void
  onToggleSeasonType: (seasonType: LeaderboardSeasonType) => void
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
  onToggleSeasonType,
  onSelectAllTeams,
  onToggleTeam,
  onNumberChange,
  onRefresh,
}: LeaderboardFiltersPanelProps) {
  const isDisabled = isBootstrapping || isLoading
  const hasSelectedTeams =
    availableTeams.length > 0 && (filters.teamIds === null || filters.teamIds.length > 0)
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
    <aside className="flex flex-col gap-[18px] rounded-[28px] border border-[color:var(--panel-border)] bg-[var(--panel-muted-background)] p-5 shadow-[var(--panel-shadow)] max-sm:rounded-[22px] max-sm:p-[18px]">
      <div className="flex flex-col gap-3">
        <div>
          <p className={PANEL_LABEL_CLASS_NAME}>Query</p>
        </div>
      </div>

      <div className="flex flex-row flex-wrap items-start gap-4 max-sm:flex-col">
        <div className="flex w-30 flex-col gap-3">
          <label className="flex flex-col gap-1.5 text-[0.9rem] text-[color:var(--text-secondary)]">
            <span>Start season</span>
            <select
              className="min-h-10.5 w-full rounded-xl border border-[color:var(--control-border)] bg-(--input-background) px-[14px] text-(--text-primary)"
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

          <label className="flex flex-col gap-1.5 text-[0.9rem] text-(--text-secondary)">
            <span>End season</span>
            <select
              className="min-h-10.5 w-full rounded-xl border border-(--control-border) bg-(--input-background) px-3.5 text-(--text-primary)"
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
        </div>

        <fieldset className="m-0 flex w-fit flex-col gap-2 border-0 p-0 max-sm:w-full">
          <legend className="mb-1 text-[0.9rem] text-(--text-secondary)">
            Season types
          </legend>
          <div className="flex flex-col flex-wrap gap-2">
            {SEASON_TYPE_BUTTONS.map((seasonType) => {
              const isSelected = filters.seasonTypes.includes(seasonType.key)
              const isOnlySelected = isSelected && filters.seasonTypes.length === 1
              return (
                <button
                  key={seasonType.key}
                  type="button"
                  className={
                    isSelected
                      ? 'min-h-9.5 w-36 cursor-pointer rounded-lg border border-(--accent-border) [background:var(--accent-gradient)] px-3 text-sm font-bold text-(--text-inverse) disabled:cursor-not-allowed disabled:opacity-70'
                      : 'min-h-9.5 w-36 cursor-pointer rounded-lg border border-(--control-border) bg-(--input-background) px-3 text-sm font-bold text-(--text-secondary) transition-colors disabled:cursor-not-allowed disabled:opacity-60'
                  }
                  aria-pressed={isSelected}
                  disabled={isDisabled || isOnlySelected}
                  onClick={() => onToggleSeasonType(seasonType.key)}
                >
                  {seasonType.label}
                </button>
              )
            })}
          </div>
          <p className="m-0 text-xs w-fit leading-[1.45] text-(--text-muted)">
            One season type is required.
          </p>
        </fieldset>

        <div className="w-26">
          <NumericField
            label="Top N players"
            min="1"
            value={filters.topN}
            disabled={isDisabled}
            onChange={(value) => onNumberChange('topN', value)}
          />
        </div>

        <div className="flex w-38 flex-col gap-3">
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
        </div>
        <div>
          {metricFields.map((field) => (
            <div key={field.key} className="w-34">
              <NumericField
                label={field.label}
                min="0"
                step={field.step}
                value={field.value}
                disabled={isDisabled}
                onChange={(value) => onNumberChange(field.key, value)}
              />
            </div>
          ))}

        </div>

        <fieldset className="m-0 flex min-w-[18rem] flex-1 flex-col gap-3 border-0 p-0 max-sm:min-w-0 max-sm:w-full">
          <legend className={SECTION_TITLE_CLASS_NAME}>Teams</legend>
          <TeamSelector
            availableTeams={availableTeams}
            selectedTeamIds={filters.teamIds}
            disabled={isDisabled}
            onSelectAll={onSelectAllTeams}
            onToggleTeam={onToggleTeam}
          />

          {!hasSelectedTeams ? (
            <p className="m-0 leading-[1.55] text-[color:var(--text-muted)]">
              No teams are available for the current season span.
            </p>
          ) : null}
        </fieldset>
      </div>

      <div className="mt-auto">
        <button
          type="button"
          className="min-h-[42px] w-full cursor-pointer rounded-[14px] [background:var(--accent-gradient)] px-[18px] font-bold text-[color:var(--text-inverse)] transition-opacity disabled:cursor-not-allowed disabled:opacity-60"
          onClick={onRefresh}
          disabled={
            isDisabled ||
            !filters.startSeason ||
            !filters.endSeason ||
            filters.seasonTypes.length === 0 ||
            !hasSelectedTeams
          }
        >
          {isLoading ? 'Refreshing...' : 'Refresh leaderboard'}
        </button>
      </div>
    </aside>
  )
}
