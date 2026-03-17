import { useEffect, useEffectEvent, useMemo, useState } from 'react'
import type { ChangeEvent, Dispatch, SetStateAction } from 'react'
import './App.css'

const CHART_WIDTH = 920
const CHART_HEIGHT = 420
const CHART_PADDING = { top: 24, right: 28, bottom: 48, left: 60 }
const SERIES_COLORS = [
  '#e76f51',
  '#2a9d8f',
  '#264653',
  '#f4a261',
  '#457b9d',
  '#8d99ae',
  '#ef476f',
  '#118ab2',
  '#6d597a',
  '#588157',
  '#bc6c25',
  '#7f5539',
] as const

type AppMode = 'cached' | 'custom'
type MetricId = 'wowy' | 'rawr'

type SpanPoint = {
  season: string
  value: number | null
}

type SpanSeries = {
  player_id: number
  player_name: string
  span_average_value: number
  season_count: number
  points: SpanPoint[]
}

type TableRow = {
  rank: number
  player_id: number
  player_name: string
  span_average_value: number
  average_minutes: number | null
  total_minutes: number
  games_with: number
  games_without: number
  avg_margin_with: number | null
  avg_margin_without: number | null
  season_count: number
}

type MetricFilters = {
  team: string[] | null
  season?: string[] | null
  season_type: string
  min_games_with?: number
  min_games_without?: number
  min_games?: number
  min_average_minutes: number
  min_total_minutes: number
  top_n: number
}

type LeaderboardPayload = {
  mode: AppMode
  metric: MetricId
  metric_label: string
  span: {
    start_season: string | null
    end_season: string | null
    available_seasons: string[]
    top_n: number
  }
  table_rows: TableRow[]
  series: SpanSeries[]
  filters: MetricFilters
  available_teams?: string[]
  available_seasons?: string[]
}

type MetricOptionsPayload = {
  metric: MetricId
  metric_label: string
  available_teams: string[]
  available_seasons: string[]
  filters: MetricFilters
}

type ErrorPayload = {
  error?: string
}

type ChartGridLine = {
  value: number
  y: number
}

type ChartTick = {
  season: string
  x: number
}

type ChartPoint = {
  season: string
  value: number
  x: number
  y: number
}

type ChartSeries = Omit<SpanSeries, 'points'> & {
  points: ChartPoint[]
  segments: string[]
}

type ChartModel = {
  gridLines: ChartGridLine[]
  xTicks: ChartTick[]
  series: ChartSeries[]
}

type CachedFilters = {
  team: string
  topN: number
}

type CustomFilters = {
  startSeason: string
  endSeason: string
  teams: string[]
  topN: number
  minGames: number
  minGamesWith: number
  minGamesWithout: number
  minAverageMinutes: number
  minTotalMinutes: number
}

type LoadingPhase = {
  label: string
  detail: string
}

type LoadingPanelModel = {
  title: string
  summary: string
  progressLabel: string
  progressPercent: number
  phases: LoadingPhase[]
  activePhaseIndex: number
}

function App() {
  const [metric, setMetric] = useState<MetricId>('wowy')
  const [mode, setMode] = useState<AppMode>('cached')
  const [metricLabel, setMetricLabel] = useState('WOWY')
  const [metricFilters, setMetricFilters] = useState<MetricFilters>(defaultMetricFilters('wowy'))
  const [availableTeams, setAvailableTeams] = useState<string[]>([])
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [leaderboard, setLeaderboard] = useState<LeaderboardPayload | null>(null)
  const [cachedFilters, setCachedFilters] = useState<CachedFilters>({
    team: '',
    topN: 12,
  })
  const [customFilters, setCustomFilters] = useState<CustomFilters>({
    startSeason: '',
    endSeason: '',
    teams: [],
    topN: 12,
    minGames: 35,
    minGamesWith: 15,
    minGamesWithout: 2,
    minAverageMinutes: 30,
    minTotalMinutes: 600,
  })
  const [isBootstrapping, setIsBootstrapping] = useState(true)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')
  const [loadingStartedAt, setLoadingStartedAt] = useState<number | null>(Date.now())
  const [loadingTick, setLoadingTick] = useState(0)

  const chartModel = useMemo<ChartModel>(
    () => buildChartModel(leaderboard?.series ?? []),
    [leaderboard],
  )
  const loadingPanel = useMemo<LoadingPanelModel | null>(() => {
    if (!isBootstrapping && !isLoading) {
      return null
    }
    const elapsedMs = loadingStartedAt === null ? 0 : Math.max(Date.now() - loadingStartedAt, 0)
    return buildLoadingPanelModel({
      metric,
      metricLabel,
      mode,
      isBootstrapping,
      elapsedMs,
    })
  }, [isBootstrapping, isLoading, loadingStartedAt, loadingTick, metric, metricLabel, mode])

  useEffect(() => {
    if (!isBootstrapping && !isLoading) {
      setLoadingStartedAt(null)
      return
    }
    if (loadingStartedAt !== null) {
      return
    }
    setLoadingStartedAt(Date.now())
  }, [isBootstrapping, isLoading, loadingStartedAt])

  useEffect(() => {
    if (!isBootstrapping && !isLoading) {
      return
    }
    const intervalId = window.setInterval(() => {
      setLoadingTick((current) => current + 1)
    }, 180)
    return () => window.clearInterval(intervalId)
  }, [isBootstrapping, isLoading])

  const loadOptions = useEffectEvent(async (nextMetric: MetricId) => {
    setIsBootstrapping(true)
    setError('')
    setLoadingStartedAt(Date.now())
    try {
      const payload = (await fetchJson(`/api/metrics/${nextMetric}/options`)) as MetricOptionsPayload
      const defaultStartSeason = payload.available_seasons[0] || ''
      const defaultEndSeason = payload.available_seasons[payload.available_seasons.length - 1] || ''
      setMetricLabel(payload.metric_label)
      setMetricFilters(payload.filters)
      setAvailableTeams(payload.available_teams)
      setAvailableSeasons(payload.available_seasons)
      setCachedFilters((current) => ({
        ...current,
        topN: current.topN || payload.filters.top_n,
      }))
      setCustomFilters((current) => ({
        ...current,
        startSeason: payload.available_seasons.includes(current.startSeason)
          ? current.startSeason
          : defaultStartSeason,
        endSeason: payload.available_seasons.includes(current.endSeason)
          ? current.endSeason
          : defaultEndSeason,
        topN: current.topN || payload.filters.top_n,
        minGames: payload.filters.min_games ?? current.minGames,
        minGamesWith: payload.filters.min_games_with ?? current.minGamesWith,
        minGamesWithout: payload.filters.min_games_without ?? current.minGamesWithout,
        minAverageMinutes: payload.filters.min_average_minutes,
        minTotalMinutes: payload.filters.min_total_minutes,
      }))
      return payload
    } finally {
      setIsBootstrapping(false)
    }
  })

  const loadCachedLeaderboard = useEffectEvent(
    async (nextMetric: MetricId, filtersOverride?: MetricFilters) => {
      setIsLoading(true)
      setError('')
      setLoadingStartedAt(Date.now())
      const effectiveFilters = filtersOverride ?? metricFilters
      const params = new URLSearchParams({
        top_n: String(cachedFilters.topN),
        min_average_minutes: String(effectiveFilters.min_average_minutes),
        min_total_minutes: String(effectiveFilters.min_total_minutes),
      })
      if (effectiveFilters.min_games !== undefined) {
        params.set('min_games', String(effectiveFilters.min_games))
      }
      if (effectiveFilters.min_games_with !== undefined) {
        params.set('min_games_with', String(effectiveFilters.min_games_with))
      }
      if (effectiveFilters.min_games_without !== undefined) {
        params.set('min_games_without', String(effectiveFilters.min_games_without))
      }
      if (cachedFilters.team) {
        params.set('team', cachedFilters.team)
      }

      try {
        const payload = (await fetchJson(
          `/api/metrics/${nextMetric}/cached-leaderboard?${params.toString()}`,
        )) as LeaderboardPayload
        setMetricLabel(payload.metric_label)
        setLeaderboard(payload)
      } catch (caughtError) {
        const message = caughtError instanceof Error ? caughtError.message : 'Request failed'
        setError(message)
        setLeaderboard(null)
      } finally {
        setIsLoading(false)
      }
    },
  )

  const runCustomQuery = useEffectEvent(async () => {
    setIsLoading(true)
    setError('')
    setLoadingStartedAt(Date.now())
    const params = new URLSearchParams({
      top_n: String(customFilters.topN),
      min_average_minutes: String(customFilters.minAverageMinutes),
      min_total_minutes: String(customFilters.minTotalMinutes),
    })
    if (metric === 'rawr') {
      params.set('min_games', String(customFilters.minGames))
    } else {
      params.set('min_games_with', String(customFilters.minGamesWith))
      params.set('min_games_without', String(customFilters.minGamesWithout))
    }

    for (const team of customFilters.teams) {
      params.append('team', team)
    }
    for (const season of seasonSpan(customFilters.startSeason, customFilters.endSeason, availableSeasons)) {
      params.append('season', season)
    }

    try {
      const payload = (await fetchJson(
        `/api/metrics/${metric}/custom-query?${params.toString()}`,
      )) as LeaderboardPayload
      setMetricLabel(payload.metric_label)
      setLeaderboard(payload)
    } catch (caughtError) {
      const message = caughtError instanceof Error ? caughtError.message : 'Request failed'
      setError(message)
      setLeaderboard(null)
    } finally {
      setIsLoading(false)
    }
  })

  useEffect(() => {
    void (async () => {
      const options = await loadOptions(metric)
      await loadCachedLeaderboard(metric, options.filters)
    })()
  }, [metric])

  const seasonSummary = leaderboard?.span.available_seasons.length
    ? `${leaderboard.span.available_seasons[0]} to ${leaderboard.span.available_seasons.at(-1)}`
    : 'No seasons loaded'
  const metricDescription =
    metric === 'wowy'
      ? 'Cross-season on/off impact from with-and-without samples.'
      : 'Game-level ridge model of player impact across the cached history.'
  const chartStatusLabel =
    mode === 'cached' ? `Loading cached ${metricLabel} leaders...` : `Running ${metricLabel} query...`
  const resultsTitle =
    mode === 'cached'
      ? `Top ${leaderboard?.span.top_n ?? cachedFilters.topN} cached ${metricLabel} leaders`
      : `Top ${leaderboard?.span.top_n ?? customFilters.topN} players for this custom ${metricLabel} query`

  return (
    <main className="page-shell">
      <section className="hero-panel">
        <div className="hero-copy">
          <div>
            <h1>{metricLabel} Leaderboard</h1>
            <p className="lede">
              {metricDescription}
            </p>
          </div>
          <div className="metric-select">
            <div className="mode-toggle" role="tablist" aria-label="Metric selector">
              <button
                type="button"
                className={metric === 'rawr' ? 'mode-tab active' : 'mode-tab'}
                onClick={() => setMetric('rawr')}
              >
                RAWR
              </button>
              <button
                type="button"
                className={metric === 'wowy' ? 'mode-tab active' : 'mode-tab'}
                onClick={() => setMetric('wowy')}
              >
                WOWY
              </button>
            </div>
          </div>
        </div>
      </section>



      <section className="mode-panel">
        <div className="mode-toggle" role="tablist" aria-label="Query mode">
          <button
            type="button"
            className={mode === 'cached' ? 'mode-tab active' : 'mode-tab'}
            onClick={() => setMode('cached')}
          >
            All Time Leaders
          </button>
          <button
            type="button"
            className={mode === 'custom' ? 'mode-tab active' : 'mode-tab'}
            onClick={() => setMode('custom')}
          >
            Custom query
          </button>
        </div>
      </section>

      {mode === 'cached' ? (
        <section className="control-panel">
          <label>
            <span>Team scope</span>
            <select
              value={cachedFilters.team}
              onChange={(event) =>
                setCachedFilters((current) => ({ ...current, team: event.target.value }))
              }
              disabled={isBootstrapping || isLoading}
            >
              <option value="">All teams</option>
              {availableTeams.map((team) => (
                <option key={team} value={team}>
                  {team}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>Cached span</span>
            <output className="control-output">{seasonSummary}</output>
          </label>

          <label>
            <span>Top players</span>
            <input
              type="number"
              min="1"
              max="50"
              value={cachedFilters.topN}
              onChange={(event) =>
                setCachedFilters((current) => ({
                  ...current,
                  topN: Number(event.target.value),
                }))
              }
            />
          </label>

          <button
            type="button"
            className="run-button"
            onClick={() => void loadCachedLeaderboard(metric)}
            disabled={isBootstrapping || isLoading}
          >
            {isLoading ? 'Loading...' : 'Refresh leaders'}
          </button>
        </section>
      ) : (
        <section className="query-panel">
          <label>
            <span>Start season</span>
            <select
              value={customFilters.startSeason}
              onChange={(event) =>
                setCustomFilters((current) => ({
                  ...current,
                  startSeason: event.target.value,
                }))
              }
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
              onChange={(event) =>
                setCustomFilters((current) => ({
                  ...current,
                  endSeason: event.target.value,
                }))
              }
              disabled={isBootstrapping || isLoading}
            >
              {availableSeasons.map((season) => (
                <option key={season} value={season}>
                  {season}
                </option>
              ))}
            </select>
          </label>

          <label className="query-multi">
            <span>Teams</span>
            <select
              multiple
              value={customFilters.teams}
              onChange={handleCustomTeamsChange(setCustomFilters)}
              disabled={isBootstrapping || isLoading}
            >
              {availableTeams.map((team) => (
                <option key={team} value={team}>
                  {team}
                </option>
              ))}
            </select>
            <small>Leave empty to query all teams in the selected span.</small>
          </label>

          <label>
            <span>Top players</span>
            <input
              type="number"
              min="1"
              max="50"
              value={customFilters.topN}
              onChange={(event) => updateCustomNumber(setCustomFilters, 'topN', event)}
            />
          </label>

          <label>
            <span>{metric === 'rawr' ? 'Min games' : 'Min games with'}</span>
            <input
              type="number"
              min="0"
              value={metric === 'rawr' ? customFilters.minGames : customFilters.minGamesWith}
              onChange={(event) =>
                updateCustomNumber(
                  setCustomFilters,
                  metric === 'rawr' ? 'minGames' : 'minGamesWith',
                  event,
                )
              }
            />
          </label>

          {metric === 'wowy' ? (
            <label>
              <span>Min games without</span>
              <input
                type="number"
                min="0"
                value={customFilters.minGamesWithout}
                onChange={(event) => updateCustomNumber(setCustomFilters, 'minGamesWithout', event)}
              />
            </label>
          ) : null}

          <label>
            <span>Min average minutes</span>
            <input
              type="number"
              min="0"
              step="0.5"
              value={customFilters.minAverageMinutes}
              onChange={(event) =>
                updateCustomNumber(setCustomFilters, 'minAverageMinutes', event)
              }
            />
          </label>

          <label>
            <span>Min total minutes</span>
            <input
              type="number"
              min="0"
              step="10"
              value={customFilters.minTotalMinutes}
              onChange={(event) => updateCustomNumber(setCustomFilters, 'minTotalMinutes', event)}
            />
          </label>

          <button
            type="button"
            className="run-button query-run"
            onClick={() => void runCustomQuery()}
            disabled={
              isBootstrapping ||
              isLoading ||
              !customFilters.startSeason ||
              !customFilters.endSeason
            }
          >
            {isLoading ? 'Running...' : 'Run query'}
          </button>
        </section>
      )}

      <section className="chart-panel">
        <div className="chart-header">
          <div>
            <p className="panel-label">{mode === 'cached' ? 'Cached board' : 'Custom run'}</p>
            <h2>{leaderboard?.span.start_season ? seasonSummary : `${metricLabel} results`}</h2>
          </div>
          {leaderboard ? (
            <div className="chart-meta">
              <span>{leaderboard.table_rows.length} series</span>
              <span>{leaderboard.span.available_seasons.length} seasons</span>
              <span>{leaderboard.mode === 'cached' ? 'Cached' : 'Recalculated live'}</span>
            </div>
          ) : null}
        </div>

        {error ? <p className="status error">{error}</p> : null}
        {!error && loadingPanel ? (
          <section className="status status-loading" aria-live="polite">
            <div className="status-progress-header">
              <div>
                <p className="panel-label">Live status</p>
                <h3>{loadingPanel.title}</h3>
              </div>
              <strong>{loadingPanel.progressPercent}%</strong>
            </div>
            <p className="status-summary">{loadingPanel.summary}</p>
            <div
              className="status-progress-track"
              role="progressbar"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={loadingPanel.progressPercent}
              aria-label={loadingPanel.progressLabel}
            >
              <div
                className="status-progress-fill"
                style={{ width: `${loadingPanel.progressPercent}%` }}
              />
            </div>
            <p className="status-progress-label">{loadingPanel.progressLabel}</p>
            <div className="status-phase-list">
              {loadingPanel.phases.map((phase, index) => (
                <article
                  key={phase.label}
                  className={
                    index === loadingPanel.activePhaseIndex ? 'status-phase active' : 'status-phase'
                  }
                >
                  <strong>{phase.label}</strong>
                  <p>{phase.detail}</p>
                </article>
              ))}
            </div>
          </section>
        ) : null}
        {!error && !loadingPanel && (isBootstrapping || isLoading) ? (
          <p className="status">{chartStatusLabel}</p>
        ) : null}
        {!error && !isLoading && !leaderboard ? (
          <p className="status">No leaderboard data loaded yet.</p>
        ) : null}
        {!error && !isLoading && leaderboard && leaderboard.table_rows.length === 0 ? (
          <p className="status">No players matched the current filters.</p>
        ) : null}
        {!error && !isLoading && leaderboard && leaderboard.table_rows.length > 0 ? (
          <>
            <div className="chart-frame">
              <svg
                className="wowy-chart"
                viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`}
                role="img"
                aria-label={`${metricLabel} line chart by season`}
              >
                {chartModel.gridLines.map((line) => (
                  <g key={line.value}>
                    <line
                      x1={CHART_PADDING.left}
                      x2={CHART_WIDTH - CHART_PADDING.right}
                      y1={line.y}
                      y2={line.y}
                      className="grid-line"
                    />
                    <text x={18} y={line.y + 4} className="axis-label">
                      {line.value.toFixed(1)}
                    </text>
                  </g>
                ))}

                {chartModel.xTicks.map((tick) => (
                  <g key={tick.season}>
                    <line
                      x1={tick.x}
                      x2={tick.x}
                      y1={CHART_PADDING.top}
                      y2={CHART_HEIGHT - CHART_PADDING.bottom}
                      className="grid-line grid-line-vertical"
                    />
                    <text
                      x={tick.x}
                      y={CHART_HEIGHT - 16}
                      textAnchor="middle"
                      className="axis-label"
                    >
                      {tick.season}
                    </text>
                  </g>
                ))}

                {chartModel.series.map((series, index) => (
                  <g key={series.player_id}>
                    {series.segments.map((segment, segmentIndex) => (
                      <polyline
                        key={`${series.player_id}-${segmentIndex}`}
                        points={segment}
                        fill="none"
                        stroke={SERIES_COLORS[index % SERIES_COLORS.length]}
                        strokeWidth="3"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      />
                    ))}
                    {series.points.map((point) => (
                      <g key={`${series.player_id}-${point.season}`}>
                        <circle
                          cx={point.x}
                          cy={point.y}
                          r="4.5"
                          fill={SERIES_COLORS[index % SERIES_COLORS.length]}
                        />
                        <title>
                          {series.player_name} {point.season}: {point.value.toFixed(2)}
                        </title>
                      </g>
                    ))}
                  </g>
                ))}
              </svg>
            </div>

            <div className="results-table-panel">
              <div className="table-header">
                <div>
                  <p className="panel-label">Ranked table</p>
                  <h3>{resultsTitle}</h3>
                </div>
              </div>
              <div className="results-table-frame">
                <table className="results-table">
                  <thead>
                    <tr>
                      <th>Rank</th>
                      <th>Player</th>
                      <th>{`${metricLabel}`}</th>
                      <th>Seasons</th>
                      <th>Avg Min</th>
                      <th>Tot Min</th>
                      {metric === 'wowy' ? (
                        <>
                          <th>With</th>
                          <th>Without</th>
                          <th>Avg With</th>
                          <th>Avg Without</th>
                        </>
                      ) : (
                        <th>Games</th>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {leaderboard.table_rows.map((row) => (
                      <tr key={row.player_id}>
                        <td>{row.rank}</td>
                        <td>{row.player_name}</td>
                        <td>{formatNumber(row.span_average_value, 2)}</td>
                        <td>{row.season_count}</td>
                        <td>{formatNumber(row.average_minutes, 1)}</td>
                        <td>{formatNumber(row.total_minutes, 1)}</td>
                        {metric === 'wowy' ? (
                          <>
                            <td>{row.games_with}</td>
                            <td>{row.games_without}</td>
                            <td>{formatNumber(row.avg_margin_with, 2)}</td>
                            <td>{formatNumber(row.avg_margin_without, 2)}</td>
                          </>
                        ) : (
                          <td>{row.games_with}</td>
                        )}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : null}
      </section>
    </main>
  )
}

function defaultMetricFilters(metric: MetricId): MetricFilters {
  if (metric === 'rawr') {
    return {
      team: null,
      season_type: 'Regular Season',
      min_games: 35,
      min_average_minutes: 30,
      min_total_minutes: 600,
      top_n: 30,
    }
  }
  return {
    team: null,
    season_type: 'Regular Season',
    min_games_with: 15,
    min_games_without: 2,
    min_average_minutes: 30,
    min_total_minutes: 600,
    top_n: 30,
  }
}

function buildLoadingPanelModel({
  metric,
  metricLabel,
  mode,
  isBootstrapping,
  elapsedMs,
}: {
  metric: MetricId
  metricLabel: string
  mode: AppMode
  isBootstrapping: boolean
  elapsedMs: number
}): LoadingPanelModel {
  const phases = buildLoadingPhases(metric, mode, isBootstrapping)
  const cappedProgress = isBootstrapping
    ? Math.min(72, 14 + Math.floor(elapsedMs / 180))
    : Math.min(92, 22 + Math.floor(elapsedMs / 220))
  const activePhaseIndex = Math.min(
    phases.length - 1,
    Math.floor((cappedProgress / 100) * phases.length),
  )
  const title = isBootstrapping
    ? `Opening ${metricLabel} data pipeline`
    : mode === 'custom'
      ? `Running ${metricLabel} custom query`
      : `Refreshing cached ${metricLabel} leaderboard`

  return {
    title,
    summary:
      phases[activePhaseIndex]?.detail ??
      `Loading ${metricLabel} data from the backend and rebuilding the chart payload.`,
    progressLabel: `${cappedProgress}% complete`,
    progressPercent: cappedProgress,
    phases,
    activePhaseIndex,
  }
}

function buildLoadingPhases(
  metric: MetricId,
  mode: AppMode,
  isBootstrapping: boolean,
): LoadingPhase[] {
  if (isBootstrapping) {
    return [
      {
        label: 'Inspecting scope',
        detail: `Checking which cached teams and seasons are available for ${metric.toUpperCase()}.`,
      },
      {
        label: 'Reading defaults',
        detail: 'Loading the recommended filters so the first render matches the current metric store.',
      },
      {
        label: 'Preparing board',
        detail: 'Requesting the first leaderboard payload and translating it into chart-ready series.',
      },
    ]
  }

  if (metric === 'rawr') {
    if (mode === 'custom') {
      return [
        {
          label: 'Gathering sample',
          detail: 'Collecting the requested team and season slice from the normalized RAWR inputs.',
        },
        {
          label: 'Fitting ridge',
          detail: 'Running the game-level ridge regression and applying the minimum games threshold.',
        },
        {
          label: 'Ranking span',
          detail: 'Aggregating the player-season coefficients into the final span leaderboard and chart points.',
        },
      ]
    }
    return [
      {
        label: 'Loading scope',
        detail: 'Reading the prebuilt RAWR regression store for the selected team scope and season type.',
      },
      {
        label: 'Filtering rows',
        detail: 'Applying the minimum games and minute thresholds before ranking the remaining player seasons.',
      },
      {
        label: 'Rendering chart',
        detail: 'Rebuilding the multi-season series and ranked table for the frontend.',
      },
    ]
  }

  if (mode === 'custom') {
    return [
      {
        label: 'Gathering sample',
        detail: 'Collecting the requested team and season slice from the cached WOWY inputs.',
      },
      {
        label: 'Running WOWY',
        detail: 'Computing with/without impact for each player across the selected game sample.',
      },
      {
        label: 'Ranking span',
        detail: 'Aggregating the player-season results into the final span leaderboard and chart points.',
      },
    ]
  }

  return [
    {
      label: 'Loading cache',
      detail: 'Reading cached WOWY player-season rows for the selected scope.',
    },
    {
      label: 'Applying filters',
      detail: 'Filtering by minutes and sample sizes before ranking the strongest multi-season profiles.',
    },
    {
      label: 'Rendering board',
      detail: 'Building the chart series and leaderboard table for the current span.',
    },
  ]
}

function handleCustomTeamsChange(setCustomFilters: Dispatch<SetStateAction<CustomFilters>>) {
  return (event: ChangeEvent<HTMLSelectElement>) => {
    const teams = [...event.target.selectedOptions].map((option) => option.value)
    setCustomFilters((current) => ({ ...current, teams }))
  }
}

function updateCustomNumber(
  setCustomFilters: Dispatch<SetStateAction<CustomFilters>>,
  field: keyof Pick<
    CustomFilters,
    | 'topN'
    | 'minGames'
    | 'minGamesWith'
    | 'minGamesWithout'
    | 'minAverageMinutes'
    | 'minTotalMinutes'
  >,
  event: ChangeEvent<HTMLInputElement>,
) {
  const nextValue = Number(event.target.value)
  setCustomFilters((current) => ({
    ...current,
    [field]: Number.isFinite(nextValue) ? nextValue : 0,
  }))
}

function seasonSpan(startSeason: string, endSeason: string, seasons: string[]): string[] {
  if (!startSeason || !endSeason) {
    return []
  }
  const startIndex = seasons.indexOf(startSeason)
  const endIndex = seasons.indexOf(endSeason)
  if (startIndex === -1 || endIndex === -1) {
    return []
  }
  const lowIndex = Math.min(startIndex, endIndex)
  const highIndex = Math.max(startIndex, endIndex)
  return seasons.slice(lowIndex, highIndex + 1)
}

async function fetchJson(url: string): Promise<unknown> {
  const response = await fetch(url)
  const contentType = response.headers.get('content-type') ?? ''
  const bodyText = await response.text()

  if (!contentType.includes('application/json')) {
    if (response.status >= 500 || bodyText.trimStart().startsWith('<')) {
      throw new Error(
        'The web API is unavailable or returned HTML. Start the backend with `poetry run wowy-web`.',
      )
    }
    throw new Error(bodyText || 'Request failed')
  }

  const payload = JSON.parse(bodyText) as unknown
  if (!response.ok) {
    const errorPayload = payload as ErrorPayload
    throw new Error(errorPayload.error ?? `Request failed (${response.status})`)
  }
  return payload
}

function buildChartModel(series: SpanSeries[]): ChartModel {
  const seasons = uniqueSeasons(series)
  const scoredPoints = series.flatMap((entry) =>
    entry.points.filter((point): point is ChartPointBase => point.value !== null),
  )

  if (seasons.length === 0 || scoredPoints.length === 0) {
    return { gridLines: [], xTicks: [], series: [] }
  }

  const minScore = Math.min(...scoredPoints.map((point) => point.value))
  const maxScore = Math.max(...scoredPoints.map((point) => point.value))
  const spread = maxScore - minScore || 1
  const yMin = minScore - spread * 0.15
  const yMax = maxScore + spread * 0.15
  const chartInnerWidth = CHART_WIDTH - CHART_PADDING.left - CHART_PADDING.right
  const chartInnerHeight = CHART_HEIGHT - CHART_PADDING.top - CHART_PADDING.bottom
  const seasonIndex = new Map(seasons.map((season, index) => [season, index]))

  const xForSeason = (index: number) =>
    CHART_PADDING.left +
    (seasons.length === 1 ? chartInnerWidth / 2 : (index / (seasons.length - 1)) * chartInnerWidth)

  const yForScore = (score: number) =>
    CHART_PADDING.top + ((yMax - score) / (yMax - yMin || 1)) * chartInnerHeight

  return {
    gridLines: buildGridLines(yMin, yMax, yForScore),
    xTicks: seasons.map((season, index) => ({ season, x: xForSeason(index) })),
    series: series.map<ChartSeries>((entry) => {
      const points: ChartPoint[] = entry.points
        .filter((point): point is ChartPointBase => point.value !== null)
        .map((point) => {
          const index = seasonIndex.get(point.season)
          if (index === undefined) {
            throw new Error(`Unknown season ${point.season}`)
          }
          return {
            ...point,
            x: xForSeason(index),
            y: yForScore(point.value),
          }
        })

      return {
        ...entry,
        points,
        segments: toSegments(points),
      }
    }),
  }
}

type ChartPointBase = {
  season: string
  value: number
}

function buildGridLines(
  yMin: number,
  yMax: number,
  yForScore: (score: number) => number,
): ChartGridLine[] {
  const steps = 5
  return Array.from({ length: steps + 1 }, (_, index) => {
    const value = yMin + ((yMax - yMin) / steps) * index
    return { value, y: yForScore(value) }
  })
}

function toSegments(points: ChartPoint[]): string[] {
  if (points.length === 0) {
    return []
  }

  return [points.map((point) => `${point.x},${point.y}`).join(' ')]
}

function uniqueSeasons(series: SpanSeries[]): string[] {
  return [...new Set(series.flatMap((entry) => entry.points.map((point) => point.season)))]
}

function formatNumber(value: number | null, decimals: number): string {
  if (value === null) {
    return '-'
  }
  return value.toFixed(decimals)
}

export default App
