import { LeaderboardChart } from './LeaderboardChart'
import { LoadingStatus } from './LoadingStatus'
import { ResultsTable } from './ResultsTable'
import type { LeaderboardPayload } from '../app/leaderboardApiTypes'
import type { LoadingPanelModel } from '../app/loadingTypes'
import type { MetricId } from '../app/metricTypes'

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
    <section className="results-panel">
      <header className="results-panel__header">
        <div>
          <p className="panel-label">Leaderboard</p>
          <h2>{leaderboard?.span.start_season ? seasonSummary : `${metricLabel} results`}</h2>
        </div>

        {leaderboard ? (
          <div className="results-panel__meta">
            <span>{leaderboard.table_rows.length} players</span>
            <span>{leaderboard.span.available_seasons.length} seasons</span>
            <span>{leaderboard.mode === 'cache' ? 'Served from cache' : 'Calculated live'}</span>
          </div>
        ) : null}
      </header>

      {error ? <p className="status-card status-card--error">{error}</p> : null}

      {!error && loadingPanel ? <LoadingStatus model={loadingPanel} /> : null}

      {!error && !loadingPanel && (isBootstrapping || isLoading) ? (
        <p className="status-card">{`Loading ${metricLabel} leaderboard...`}</p>
      ) : null}

      {!error && !isLoading && !leaderboard ? (
        <p className="status-card">No leaderboard data loaded yet.</p>
      ) : null}

      {!error && !isLoading && leaderboard && leaderboard.table_rows.length === 0 ? (
        <p className="status-card">No players matched the current filters.</p>
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
