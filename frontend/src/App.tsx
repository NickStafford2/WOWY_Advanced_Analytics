import { useEffect, useEffectEvent, useMemo, useState } from 'react'
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
  span_average_value: number
  season_count: number
  points: SpanPoint[]
}

type PlayerSeasonRow = {
  season: string
  player_id: number
  player_name: string
  value: number
  sample_size: number | null
  secondary_sample_size: number | null
  games_with?: number
  games_without?: number
  avg_margin_with?: number
  avg_margin_without?: number
  average_minutes: number | null
  total_minutes: number | null
}

type PlayerSeasonsPayload = {
  metric: string
  metric_label: string
  rows: PlayerSeasonRow[]
}

type MetricOptionsPayload = {
  metric: string
  metric_label: string
  available_teams: string[]
  available_seasons: string[]
  filters: {
    team: string[] | null
    season_type: string
    min_games_with: number
    min_games_without: number
    min_average_minutes: number
    min_total_minutes: number
    top_n: number
  }
}

type ErrorPayload = {
  error?: string
}

type LoadChartOptions = {
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

function App() {
  const [availableTeams, setAvailableTeams] = useState<string[]>([])
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [selectedTeam, setSelectedTeam] = useState('')
  const [topN, setTopN] = useState(12)
  const [playerSeasonRows, setPlayerSeasonRows] = useState<PlayerSeasonRow[]>([])
  const [metricLabel, setMetricLabel] = useState('WOWY')
  const [isBootstrapping, setIsBootstrapping] = useState(true)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')

  const loadChart = useEffectEvent(async (options: LoadChartOptions) => {
    const { nextTopN = topN } = options

    setIsLoading(true)
    setError('')

    const params = new URLSearchParams({
      min_games_with: '15',
      min_games_without: '2',
      min_average_minutes: '30',
      min_total_minutes: '600',
      top_n: String(nextTopN),
    })

    if (selectedTeam) {
      params.set('team', selectedTeam)
    }

    try {
      const response = await fetch(`/api/metrics/wowy/player-seasons?${params.toString()}`)
      const payload = (await response.json()) as unknown

      if (!response.ok) {
        const errorPayload = payload as ErrorPayload
        throw new Error(errorPayload.error ?? 'Request failed')
      }
      const playerSeasonsPayload = payload as PlayerSeasonsPayload
      setMetricLabel(playerSeasonsPayload.metric_label)
      setTopN(nextTopN)
      setPlayerSeasonRows(playerSeasonsPayload.rows)
    } catch (caughtError) {
      const message =
        caughtError instanceof Error ? caughtError.message : 'Request failed'
      setError(message)
      setPlayerSeasonRows([])
    } finally {
      setIsLoading(false)
    }
  })

  const loadOptions = useEffectEvent(
    async ({ nextTeam, bootstrapChart }: { nextTeam: string; bootstrapChart: boolean }) => {
      setIsBootstrapping(true)
      setError('')

      const params = new URLSearchParams()
      if (nextTeam) {
        params.set('team', nextTeam)
      }

      try {
        const query = params.toString()
        const response = await fetch(
          `/api/metrics/wowy/options${query ? `?${query}` : ''}`,
        )
        const payload = (await response.json()) as unknown

        if (!response.ok) {
          const errorPayload = payload as ErrorPayload
          throw new Error(errorPayload.error ?? 'Request failed')
        }

        const optionsPayload = payload as MetricOptionsPayload
        setMetricLabel(optionsPayload.metric_label)
        setAvailableTeams(optionsPayload.available_teams)
        setAvailableSeasons(optionsPayload.available_seasons)
        setSelectedTeam(nextTeam)
        setTopN((currentTopN) => currentTopN || optionsPayload.filters.top_n)

        if (bootstrapChart) {
          await loadChart({ nextTopN: topN })
        }
      } catch (caughtError) {
        const message =
          caughtError instanceof Error ? caughtError.message : 'Request failed'
        setError(message)
        setPlayerSeasonRows([])
        setAvailableSeasons([])
      } finally {
        setIsBootstrapping(false)
      }
    },
  )

  useEffect(() => {
    void loadOptions({ nextTeam: '', bootstrapChart: true })
  }, [])

  const tableRows = useMemo<TableRow[]>(
    () => buildTableRows(playerSeasonRows).slice(0, topN),
    [playerSeasonRows, topN],
  )
  const displaySeries = useMemo<SpanSeries[]>(
    () => buildDisplaySeries(tableRows, playerSeasonRows, availableSeasons),
    [tableRows, playerSeasonRows, availableSeasons],
  )
  const chartModel = useMemo<ChartModel>(
    () => buildChartModel(displaySeries),
    [displaySeries],
  )

  function handleTopNChange(event: ChangeEvent<HTMLInputElement>) {
    setTopN(Number(event.target.value))
  }

  function handleTeamChange(event: ChangeEvent<HTMLSelectElement>) {
    const nextTeam = event.target.value
    void loadOptions({ nextTeam, bootstrapChart: false })
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
            <strong>{metricLabel}</strong>
            <small>Top players ranked by span-average metric value</small>
          </div>
        </div>
      </section>

      <section className="control-panel">
        <label>
          <span>Team</span>
          <select value={selectedTeam} onChange={handleTeamChange} disabled={isBootstrapping}>
            <option value="">All teams</option>
            {availableTeams.map((team) => (
              <option key={team} value={team}>
                {team}
              </option>
            ))}
          </select>
        </label>

        <label>
          <span>Cached seasons</span>
          <output className="control-output">
            {availableSeasons.length === 0
              ? 'No seasons loaded'
              : `${availableSeasons[0]} to ${availableSeasons.at(-1)}`}
          </output>
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
          disabled={
            isLoading ||
            isBootstrapping ||
            availableSeasons.length === 0
          }
        >
          {isLoading || isBootstrapping ? 'Loading...' : 'Update chart'}
        </button>
      </section>

      <section className="chart-panel">
        <div className="chart-header">
          <div>
            <p className="panel-label">Line chart</p>
            <h2>
              {availableSeasons.length > 0
                ? `${availableSeasons[0]} to ${availableSeasons.at(-1)}`
                : 'Full cached history'}
            </h2>
          </div>
          {playerSeasonRows.length > 0 ? (
            <div className="chart-meta">
              <span>{tableRows.length} series</span>
              <span>{availableSeasons.length} seasons loaded</span>
            </div>
          ) : null}
        </div>

        {error ? <p className="status error">{error}</p> : null}
        {!error && (isLoading || isBootstrapping) ? (
          <p className="status">Loading WOWY chart...</p>
        ) : null}
        {!error && !isLoading && playerSeasonRows.length === 0 ? (
          <p className="status">No players matched the current filters.</p>
        ) : null}
        {!error && !isLoading && tableRows.length > 0 ? (
          <>
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

            </div>

            <div className="results-table-panel">
              <div className="table-header">
                <div>
                  <p className="panel-label">Ranked table</p>
                  <h3>Top {topN} players by multi-season WOWY profile</h3>
                </div>
              </div>
              <div className="results-table-frame">
                <table className="results-table">
                  <thead>
                    <tr>
                      <th>Rank</th>
                      <th>Player</th>
                      <th>Player ID</th>
                      <th>Span Avg WOWY</th>
                      <th>Seasons</th>
                      <th>Avg Min</th>
                      <th>Tot Min</th>
                      <th>With</th>
                      <th>Without</th>
                      <th>Avg With</th>
                      <th>Avg Without</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tableRows.map((row) => (
                      <tr key={row.player_id}>
                        <td>{row.rank}</td>
                        <td>{row.player_name}</td>
                        <td>{row.player_id}</td>
                        <td>{formatNumber(row.span_average_value, 2)}</td>
                        <td>{row.season_count}</td>
                        <td>{formatNumber(row.average_minutes, 1)}</td>
                        <td>{formatNumber(row.total_minutes, 1)}</td>
                        <td>{row.games_with}</td>
                        <td>{row.games_without}</td>
                        <td>{formatNumber(row.avg_margin_with, 2)}</td>
                        <td>{formatNumber(row.avg_margin_without, 2)}</td>
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

function buildTableRows(playerSeasonRows: PlayerSeasonRow[]): TableRow[] {
  const rowsByPlayer = new Map<number, PlayerSeasonRow[]>()
  for (const row of playerSeasonRows) {
    rowsByPlayer.set(row.player_id, [...(rowsByPlayer.get(row.player_id) ?? []), row])
  }
  const fullSpanLength = new Set(playerSeasonRows.map((row) => row.season)).size || 1

  const rows = [...rowsByPlayer.entries()].map(([playerId, playerRows]) => {
    const playerName = playerRows[0]?.player_name ?? String(playerId)
    const gamesWith = sumBy(playerRows, (row) => row.games_with ?? row.sample_size ?? 0)
    const gamesWithout = sumBy(
      playerRows,
      (row) => row.games_without ?? row.secondary_sample_size ?? 0,
    )
    const totalMinutes = sumBy(playerRows, (row) => row.total_minutes ?? 0)
    const averageMinutes = gamesWith > 0 ? totalMinutes / gamesWith : null
    const avgMarginWith = weightedAverage(
      playerRows,
      (row) => row.avg_margin_with ?? null,
      (row) => row.games_with ?? row.sample_size ?? 0,
    )
    const avgMarginWithout = weightedAverage(
      playerRows,
      (row) => row.avg_margin_without ?? null,
      (row) => row.games_without ?? row.secondary_sample_size ?? 0,
    )
    const spanAverageValue = sumBy(playerRows, (row) => row.value) / fullSpanLength

    return {
      rank: 0,
      player_id: playerId,
      player_name: playerName,
      span_average_value: spanAverageValue,
      average_minutes: averageMinutes,
      total_minutes: totalMinutes,
      games_with: gamesWith,
      games_without: gamesWithout,
      avg_margin_with: avgMarginWith,
      avg_margin_without: avgMarginWithout,
      season_count: playerRows.length,
    }
  })

  rows.sort((left, right) => {
    const leftScore = left.span_average_value
    const rightScore = right.span_average_value
    if (rightScore !== leftScore) {
      return rightScore - leftScore
    }
    return left.player_name.localeCompare(right.player_name)
  })

  return rows.map((row, index) => ({ ...row, rank: index + 1 }))
}

function buildDisplaySeries(
  tableRows: TableRow[],
  playerSeasonRows: PlayerSeasonRow[],
  seasons: string[],
): SpanSeries[] {
  const valuesByPlayer = new Map<number, Map<string, number>>()
  for (const row of playerSeasonRows) {
    if (!valuesByPlayer.has(row.player_id)) {
      valuesByPlayer.set(row.player_id, new Map())
    }
    valuesByPlayer.get(row.player_id)?.set(row.season, row.value)
  }

  return tableRows.map((row) => ({
    player_id: row.player_id,
    player_name: row.player_name,
    span_average_value: row.span_average_value,
    season_count: row.season_count,
    points: seasons.map((season) => ({
      season,
      value: valuesByPlayer.get(row.player_id)?.get(season) ?? null,
    })),
  }))
}

function sumBy<T>(items: T[], fn: (item: T) => number): number {
  return items.reduce((total, item) => total + fn(item), 0)
}

function weightedAverage<T>(
  items: T[],
  valueFn: (item: T) => number | null,
  weightFn: (item: T) => number,
): number | null {
  let weightedTotal = 0
  let weightTotal = 0

  for (const item of items) {
    const value = valueFn(item)
    const weight = weightFn(item)
    if (value === null || weight <= 0) {
      continue
    }
    weightedTotal += value * weight
    weightTotal += weight
  }

  return weightTotal > 0 ? weightedTotal / weightTotal : null
}

function formatNumber(value: number | null, decimals: number): string {
  if (value === null) {
    return '-'
  }
  return value.toFixed(decimals)
}

export default App
