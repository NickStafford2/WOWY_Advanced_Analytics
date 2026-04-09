import { useEffect, useState } from 'react'
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
  const [visiblePlayerCount, setVisiblePlayerCount] = useState(12)
  const seasonSummary = leaderboard?.span.available_seasons.length
    ? `${leaderboard.span.available_seasons[0]} to ${leaderboard.span.available_seasons.at(-1)}`
    : 'No seasons loaded'
  const displayedMetric = leaderboard?.metric ?? metric
  const isWowyStyleMetric = displayedMetric !== 'rawr'
  const visibleRows = leaderboard?.table_rows.slice(0, visiblePlayerCount) ?? []
  const visibleSeries = leaderboard?.series.slice(0, visiblePlayerCount) ?? []
  const visibleCountOptions = _buildVisibleCountOptions(leaderboard?.table_rows.length ?? 0)

  useEffect(() => {
    if (leaderboard === null) {
      return
    }

    setVisiblePlayerCount((current) => {
      const maximumVisibleCount = Math.max(1, leaderboard.table_rows.length)
      return Math.min(current, maximumVisibleCount)
    })
  }, [leaderboard])

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
            <label className="flex items-center gap-2 rounded-full bg-[var(--meta-background)] px-3 py-[9px] text-[0.88rem] font-semibold text-[color:var(--meta-text)]">
              <span>Show</span>
              <select
                className="min-h-[28px] cursor-pointer rounded-full border border-[color:var(--control-border)] bg-[var(--input-background)] px-2 text-[0.88rem] font-semibold text-[color:var(--text-primary)]"
                value={String(visiblePlayerCount)}
                onChange={(event) => setVisiblePlayerCount(Number(event.target.value))}
              >
                {visibleCountOptions.map((count) => (
                  <option key={count} value={count}>
                    {count}
                  </option>
                ))}
              </select>
            </label>
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
          <LeaderboardChart metricLabel={metricLabel} series={visibleSeries} />
          <ResultsTable
            metricLabel={metricLabel}
            exportUrl={exportUrl}
            rows={visibleRows}
            isWowyStyleMetric={isWowyStyleMetric}
          />
        </>
      ) : null}
    </section>
  )
}

function _buildVisibleCountOptions(totalRowCount: number): number[] {
  if (totalRowCount <= 0) {
    return [1]
  }

  const options = new Set<number>([10, 12, 25, 50, 100, totalRowCount])
  return [...options].filter((count) => count <= totalRowCount).sort((left, right) => left - right)
}
