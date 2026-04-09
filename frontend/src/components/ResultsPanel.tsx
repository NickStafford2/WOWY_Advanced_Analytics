import { LeaderboardChart } from './LeaderboardChart'
import { LoadingStatus } from './LoadingStatus'
import { ResultsTable } from './ResultsTable'
import type { LeaderboardPayload } from '../app/leaderboardApiTypes'
import type { LoadingPanelModel } from '../app/loadingTypes'
import type { MetricId } from '../app/metricTypes'

const PANEL_LABEL_CLASS_NAME =
  'm-0 text-xs font-bold tracking-[0.16em] uppercase text-[color:var(--accent-warm)]'
const META_BADGE_CLASS_NAME =
  'rounded-full bg-[var(--meta-background)] px-3 py-[9px] text-[0.88rem] font-semibold text-[color:var(--meta-text)]'
const STATUS_CARD_CLASS_NAME =
  'mt-[18px] rounded-[18px] bg-[var(--status-background)] p-[18px] text-[color:var(--status-text)]'

type ResultsPanelProps = {
  metric: MetricId
  metricLabel: string
  leaderboard: LeaderboardPayload | null
  exportUrl: string
  error: string
  isLoading: boolean
  isBootstrapping: boolean
  loadingPanel: LoadingPanelModel | null
}

export function ResultsPanel({
  metric,
  metricLabel,
  leaderboard,
  exportUrl,
  error,
  isLoading,
  isBootstrapping,
  loadingPanel,
}: ResultsPanelProps) {
  const seasonSummary = leaderboard?.span.available_seasons.length
    ? `${leaderboard.span.available_seasons[0]} to ${leaderboard.span.available_seasons.at(-1)}`
    : 'No seasons loaded'
  const displayedMetric = leaderboard?.metric ?? metric
  const isWowyStyleMetric = displayedMetric !== 'rawr'

  return (
    <section className="rounded-[28px] border border-[color:var(--panel-border)] bg-[var(--panel-background)] p-[22px] shadow-[var(--panel-shadow)] max-sm:rounded-[22px] max-sm:p-[18px]">
      <header className="flex items-end justify-between gap-5 max-sm:flex-col max-sm:items-start">
        <div>
          <p className={PANEL_LABEL_CLASS_NAME}>Leaderboard</p>
          <h2 className="mt-2 text-[clamp(1.8rem,3vw,2.4rem)] leading-none">
            {leaderboard?.span.start_season ? seasonSummary : `${metricLabel} results`}
          </h2>
        </div>

        {leaderboard ? (
          <div className="flex flex-wrap gap-[10px]">
            <span className={META_BADGE_CLASS_NAME}>{leaderboard.table_rows.length} players</span>
            <span className={META_BADGE_CLASS_NAME}>
              {leaderboard.span.available_seasons.length} seasons
            </span>
            <span className={META_BADGE_CLASS_NAME}>
              {leaderboard.mode === 'cache' ? 'Served from cache' : 'Calculated live'}
            </span>
          </div>
        ) : null}
      </header>

      {error ? (
        <p className={`${STATUS_CARD_CLASS_NAME} bg-[var(--status-error-background)] text-[color:var(--status-error-text)]`}>
          {error}
        </p>
      ) : null}

      {!error && loadingPanel ? <LoadingStatus model={loadingPanel} /> : null}

      {!error && !loadingPanel && (isBootstrapping || isLoading) ? (
        <p className={STATUS_CARD_CLASS_NAME}>{`Loading ${metricLabel} leaderboard...`}</p>
      ) : null}

      {!error && !isLoading && !leaderboard ? (
        <p className={STATUS_CARD_CLASS_NAME}>No leaderboard data loaded yet.</p>
      ) : null}

      {!error && !isLoading && leaderboard && leaderboard.table_rows.length === 0 ? (
        <p className={STATUS_CARD_CLASS_NAME}>No players matched the current filters.</p>
      ) : null}

      {!error && !isLoading && leaderboard && leaderboard.table_rows.length > 0 ? (
        <>
          <LeaderboardChart metricLabel={metricLabel} series={leaderboard.series} />
          <ResultsTable
            metricLabel={metricLabel}
            exportUrl={exportUrl}
            rows={leaderboard.table_rows}
            isWowyStyleMetric={isWowyStyleMetric}
          />
        </>
      ) : null}
    </section>
  )
}
