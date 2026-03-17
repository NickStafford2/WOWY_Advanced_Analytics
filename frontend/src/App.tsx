import { useEffect, useEffectEvent, useMemo, useRef, useState } from 'react'
import type { ChangeEvent, CSSProperties, Dispatch, SetStateAction } from 'react'
import { LeaderboardChart } from './components/LeaderboardChart'
import type { SpanSeries } from './components/LeaderboardChart'
import { ResultsTable } from './components/ResultsTable'
import type { ResultsTableRow } from './components/ResultsTable'
import './App.css'

const LOADING_PANEL_DELAY_MS = 250

type AppMode = 'cached' | 'custom'
type MetricId = 'wowy' | 'wowy_shrunk' | 'rawr'

type MetricFilters = {
  team: string[] | null
  season?: string[] | null
  season_type: string
  min_games_with?: number
  min_games_without?: number
  min_games?: number
  ridge_alpha?: number
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
  table_rows: ResultsTableRow[]
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
  ridgeAlpha: number
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
  const headerRef = useRef<HTMLElement | null>(null)
  const [headerHeight, setHeaderHeight] = useState(0)
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
    ridgeAlpha: 10,
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
  const [showLoadingPanel, setShowLoadingPanel] = useState(false)

  const loadingPanel = useMemo<LoadingPanelModel | null>(() => {
    if ((!isBootstrapping && !isLoading) || !showLoadingPanel) {
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
      setShowLoadingPanel(false)
      return
    }
    if (loadingStartedAt !== null) {
      return
    }
    setLoadingStartedAt(Date.now())
  }, [isBootstrapping, isLoading, loadingStartedAt])

  useEffect(() => {
    if (!isBootstrapping && !isLoading) {
      setShowLoadingPanel(false)
      return
    }
    setShowLoadingPanel(false)
    const timeoutId = window.setTimeout(() => {
      setShowLoadingPanel(true)
    }, LOADING_PANEL_DELAY_MS)
    return () => window.clearTimeout(timeoutId)
  }, [isBootstrapping, isLoading, loadingStartedAt])

  useEffect(() => {
    if ((!isBootstrapping && !isLoading) || !showLoadingPanel) {
      return
    }
    const intervalId = window.setInterval(() => {
      setLoadingTick((current) => current + 1)
    }, 180)
    return () => window.clearInterval(intervalId)
  }, [isBootstrapping, isLoading, showLoadingPanel])

  useEffect(() => {
    const headerElement = headerRef.current
    if (headerElement === null) {
      return
    }

    const updateHeaderHeight = () => {
      setHeaderHeight(headerElement.getBoundingClientRect().height)
    }

    updateHeaderHeight()
    const resizeObserver = new ResizeObserver(updateHeaderHeight)
    resizeObserver.observe(headerElement)
    return () => resizeObserver.disconnect()
  }, [])

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
        ridgeAlpha: payload.filters.ridge_alpha ?? current.ridgeAlpha,
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
      if (effectiveFilters.ridge_alpha !== undefined) {
        params.set('ridge_alpha', String(effectiveFilters.ridge_alpha))
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
      params.set('ridge_alpha', String(customFilters.ridgeAlpha))
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
  const isRawrMetric = metric === 'rawr'
  const isWowyStyleMetric = !isRawrMetric
  const metricDescription =
    metric === 'wowy'
      ? 'Cross-season on/off impact from with-and-without samples.'
      : metric === 'wowy_shrunk'
        ? 'WOWY impact with shrinkage applied to pull small-sample seasons toward the prior.'
        : 'Game-level ridge model of player impact across the cached history.'
  const chartStatusLabel =
    mode === 'cached' ? `Loading cached ${metricLabel} leaders...` : `Running ${metricLabel} query...`
  const resultsTitle =
    mode === 'cached'
      ? `Top ${leaderboard?.span.top_n ?? cachedFilters.topN} cached ${metricLabel} leaders`
      : `Top ${leaderboard?.span.top_n ?? customFilters.topN} players for this custom ${metricLabel} query`
  const pageShellStyle = {
    '--header-offset': `${headerHeight}px`,
  } as CSSProperties

  return (
    <main className="page-shell" style={pageShellStyle}>
      <section ref={headerRef} className="hero-panel">
        <div className="hero-copy">
          <div>
            <h1>{metricLabel}</h1>
            <p className="lede">
              {metricDescription}
            </p>
          </div>
          <div className='metric-container'>

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
                  className={metric === 'wowy_shrunk' ? 'mode-tab active' : 'mode-tab'}
                  onClick={() => setMetric('wowy_shrunk')}
                >
                  WOWY Shrinkage
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
            <div className='metric-select' >
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
            </div>

          </div>
        </div>
      </section>

      <div className="page-content">
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

          {isRawrMetric ? (
            <div className="query-section-heading">
              <p className="panel-label">Pre-Fit Model Filter</p>
              <p className="query-section-note">
                `Min games` is applied before regression and changes which players enter the model.
              </p>
            </div>
          ) : null}

          <label>
            <span>{isRawrMetric ? 'Min games' : 'Min games with'}</span>
            <input
              type="number"
              min="0"
              value={isRawrMetric ? customFilters.minGames : customFilters.minGamesWith}
              onChange={(event) =>
                updateCustomNumber(
                  setCustomFilters,
                  isRawrMetric ? 'minGames' : 'minGamesWith',
                  event,
                )
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
                onChange={(event) => updateCustomNumber(setCustomFilters, 'ridgeAlpha', event)}
              />
            </label>
          ) : null}

          {isWowyStyleMetric ? (
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

          {isRawrMetric ? (
            <div className="query-section-heading">
              <p className="panel-label">Post-Fit Output Filters</p>
              <p className="query-section-note">
                Minute thresholds are applied after the regression is fit and only trim the returned rows.
              </p>
            </div>
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
        {!error && !loadingPanel && showLoadingPanel && (isBootstrapping || isLoading) ? (
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
            <LeaderboardChart metricLabel={metricLabel} series={leaderboard.series} />
            <ResultsTable
              metricLabel={metricLabel}
              resultsTitle={resultsTitle}
              rows={leaderboard.table_rows}
              isWowyStyleMetric={isWowyStyleMetric}
            />
          </>
        ) : null}
        </section>
      </div>
    </main>
  )
}

function defaultMetricFilters(metric: MetricId): MetricFilters {
  if (metric === 'rawr') {
    return {
      team: null,
      season_type: 'Regular Season',
      min_games: 35,
      ridge_alpha: 10,
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
          detail: 'Running the game-level ridge regression with the selected alpha and applying the minimum games threshold.',
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
        detail:
          metric === 'wowy_shrunk'
            ? 'Collecting the requested team and season slice from the cached WOWY inputs before shrinkage is applied.'
            : 'Collecting the requested team and season slice from the cached WOWY inputs.',
      },
      {
        label: metric === 'wowy_shrunk' ? 'Applying shrinkage' : 'Running WOWY',
        detail:
          metric === 'wowy_shrunk'
            ? 'Computing with/without impact and shrinking each player-season toward the prior based on sample balance.'
            : 'Computing with/without impact for each player across the selected game sample.',
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
      detail:
        metric === 'wowy_shrunk'
          ? 'Reading cached WOWY shrinkage player-season rows for the selected scope.'
          : 'Reading cached WOWY player-season rows for the selected scope.',
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
    | 'ridgeAlpha'
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

export default App
