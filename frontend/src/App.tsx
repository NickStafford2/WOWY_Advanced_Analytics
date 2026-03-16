import { useCallback, useEffect, useMemo, useState } from 'react'
import type { ChangeEvent } from 'react'
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

type SpanPoint = {
  season: string
  value: number | null
}

type SpanSeries = {
  player_id: number
  player_name: string
  average_value: number
  season_count: number
  points: SpanPoint[]
}

type SpanChartPayload = {
  metric: string
  metric_label: string
  span: {
    start_season: string
    end_season: string
    available_seasons: string[]
    top_n: number
  }
  filters: {
    team: string[] | null
    season: string[] | null
    season_type: string
    min_games_with: number
    min_games_without: number
    min_average_minutes: number
    min_total_minutes: number
  }
  series: SpanSeries[]
}

type ErrorPayload = {
  error?: string
}

type LoadChartOptions = {
  nextStartSeason?: string
  nextEndSeason?: string
  nextTopN?: number
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

function App() {
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [startSeason, setStartSeason] = useState('')
  const [endSeason, setEndSeason] = useState('')
  const [topN, setTopN] = useState(12)
  const [chartData, setChartData] = useState<SpanChartPayload | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')

  const loadChart = useCallback((options: LoadChartOptions) => {
    const {
      nextStartSeason = startSeason,
      nextEndSeason = endSeason,
      nextTopN = topN,
    } = options

    void (async () => {
      setIsLoading(true)
      setError('')

      const params = new URLSearchParams({
        min_games_with: '1',
        min_games_without: '1',
        min_average_minutes: '0',
        min_total_minutes: '0',
        top_n: String(nextTopN),
      })

      if (nextStartSeason) {
        params.set('start_season', nextStartSeason)
      }
      if (nextEndSeason) {
        params.set('end_season', nextEndSeason)
      }

      try {
        const response = await fetch(`/api/wowy/span-chart?${params.toString()}`)
        const payload = (await response.json()) as unknown

        if (!response.ok) {
          const errorPayload = payload as ErrorPayload
          throw new Error(errorPayload.error ?? 'Request failed')
        }

        const chartPayload = payload as SpanChartPayload
        setAvailableSeasons(chartPayload.span.available_seasons)
        setStartSeason(chartPayload.span.start_season)
        setEndSeason(chartPayload.span.end_season)
        setTopN(chartPayload.span.top_n)
        setChartData(chartPayload)
      } catch (caughtError) {
        const message =
          caughtError instanceof Error ? caughtError.message : 'Request failed'
        setError(message)
        setChartData(null)
      } finally {
        setIsLoading(false)
      }
    })()
  }, [endSeason, startSeason, topN])

  useEffect(() => {
    void loadChart({})
  }, [loadChart])

  const chartModel = useMemo<ChartModel>(
    () => buildChartModel(chartData?.series ?? []),
    [chartData],
  )

  function handleTopNChange(event: ChangeEvent<HTMLInputElement>) {
    setTopN(Number(event.target.value))
  }

  return (
    <main className="page-shell">
      <section className="hero-panel">
        <p className="eyebrow">Portfolio Prototype</p>
        <div className="hero-copy">
          <div>
            <h1>WOWY span explorer</h1>
            <p className="lede">
              Pick a season range and compare the strongest multi-year WOWY lines
              across the selected span.
            </p>
          </div>
          <div className="hero-note">
            <span>Metric</span>
            <strong>{chartData?.metric_label ?? 'WOWY'}</strong>
            <small>Top players ranked by span-average metric value</small>
          </div>
        </div>
      </section>

      <section className="control-panel">
        <label>
          <span>Start season</span>
          <select
            value={startSeason}
            onChange={(event) => setStartSeason(event.target.value)}
            disabled={isLoading || availableSeasons.length === 0}
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
            value={endSeason}
            onChange={(event) => setEndSeason(event.target.value)}
            disabled={isLoading || availableSeasons.length === 0}
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
            max="30"
            value={topN}
            onChange={handleTopNChange}
          />
        </label>

        <button
          type="button"
          className="run-button"
          onClick={() => void loadChart({})}
          disabled={isLoading || !startSeason || !endSeason || startSeason > endSeason}
        >
          {isLoading ? 'Loading...' : 'Update chart'}
        </button>
      </section>

      <section className="chart-panel">
        <div className="chart-header">
          <div>
            <p className="panel-label">Line chart</p>
            <h2>
              {startSeason && endSeason ? `${startSeason} to ${endSeason}` : 'Selected span'}
            </h2>
          </div>
          {chartData ? (
            <div className="chart-meta">
              <span>{chartData.series.length} series</span>
              <span>{chartData.span.available_seasons.length} seasons loaded</span>
            </div>
          ) : null}
        </div>

        {error ? <p className="status error">{error}</p> : null}
        {!error && isLoading ? <p className="status">Loading WOWY chart...</p> : null}
        {!error && !isLoading && chartData && chartData.series.length === 0 ? (
          <p className="status">No players matched the current filters.</p>
        ) : null}
        {!error && !isLoading && chartData && chartData.series.length > 0 ? (
          <div className="chart-layout">
            <div className="chart-frame">
              <svg
                className="wowy-chart"
                viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`}
                role="img"
                aria-label="Metric leader line chart by season"
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

            <aside className="legend-panel">
              <p className="panel-label">Top players</p>
              <ul className="legend-list">
                {chartData.series.map((series, index) => (
                  <li key={series.player_id}>
                    <span
                      className="legend-swatch"
                      style={{ backgroundColor: SERIES_COLORS[index % SERIES_COLORS.length] }}
                    />
                    <div>
                      <strong>{series.player_name}</strong>
                      <small>
                        Span avg {chartData.metric_label} {series.average_value.toFixed(2)}.
                        Appeared in {series.season_count} season
                        {series.season_count === 1 ? '' : 's'}
                      </small>
                    </div>
                  </li>
                ))}
              </ul>
            </aside>
          </div>
        ) : null}
      </section>
    </main>
  )
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

export default App
