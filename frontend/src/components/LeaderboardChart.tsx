import { useMemo, useState } from 'react'
import type { SpanSeries } from '../app/types'

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

type ChartGridLine = {
  isZero: boolean
  value: number
  y: number
}

type ChartTick = {
  label: string
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

type LeaderboardChartProps = {
  metricLabel: string
  series: SpanSeries[]
}

export function LeaderboardChart({ metricLabel, series }: LeaderboardChartProps) {
  const chartModel = useMemo<ChartModel>(() => buildChartModel(series), [series])
  const [activePlayerId, setActivePlayerId] = useState<number | null>(null)

  return (
    <div className="chart-layout">
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
                className={line.isZero ? 'grid-line zero-line' : 'grid-line'}
              />
              <text x={18} y={line.y + 4} className="axis-label">
                {line.value}
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
                {tick.label}
              </text>
            </g>
          ))}

          {chartModel.series.map((entry, index) => {
            const isActive = activePlayerId === entry.player_id
            const isDimmed = activePlayerId !== null && !isActive
            const seriesColor = isDimmed
              ? 'rgba(120, 128, 136, 0.35)'
              : SERIES_COLORS[index % SERIES_COLORS.length]
            return (
              <g
                key={entry.player_id}
                className={isDimmed ? 'chart-series is-dimmed' : 'chart-series'}
              >
                {entry.segments.map((segment, segmentIndex) => (
                  <polyline
                    key={`${entry.player_id}-${segmentIndex}`}
                    className={isActive ? 'chart-line is-active' : 'chart-line'}
                    points={segment}
                    fill="none"
                    stroke={seriesColor}
                    strokeWidth={isActive ? '4.5' : '3'}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                ))}
                {entry.points.map((point) => (
                  <g key={`${entry.player_id}-${point.season}`}>
                    <circle
                      className={isActive ? 'chart-point is-active' : 'chart-point'}
                      cx={point.x}
                      cy={point.y}
                      r={isActive ? '6.5' : '4.5'}
                      fill={isDimmed ? 'rgba(120, 128, 136, 0.45)' : seriesColor}
                      onMouseEnter={() => setActivePlayerId(entry.player_id)}
                      onMouseLeave={() => setActivePlayerId(null)}
                    />
                    <title>
                      {entry.player_name} {point.season}: {point.value.toFixed(2)}
                    </title>
                  </g>
                ))}
              </g>
            )
          })}
        </svg>
      </div>
      <aside className="legend-panel" aria-label="Chart legend">
        <p className="panel-label">Legend</p>
        <ul className="legend-list">
          {chartModel.series.map((entry, index) => {
            const isActive = activePlayerId === entry.player_id
            const isDimmed = activePlayerId !== null && !isActive
            const legendClassName = isDimmed
              ? 'legend-item is-dimmed'
              : isActive
                ? 'legend-item is-active'
                : 'legend-item'
            return (
              <li
                key={`legend-${entry.player_id}`}
                className={legendClassName}
                onMouseEnter={() => setActivePlayerId(entry.player_id)}
                onMouseLeave={() => setActivePlayerId(null)}
              >
                <span
                  className="legend-swatch"
                  style={{
                    backgroundColor: isDimmed
                      ? 'rgba(120, 128, 136, 0.45)'
                      : SERIES_COLORS[index % SERIES_COLORS.length],
                  }}
                  aria-hidden="true"
                />
                <div className="legend-list-text">
                  <strong>{entry.player_name}</strong>
                  <small>{formatNumber(entry.span_average_value, 2)}</small>
                </div>
              </li>
            )
          })}
        </ul>
      </aside>
    </div>
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
  const yAxis = buildNiceYAxis(minScore, maxScore)
  const chartInnerWidth = CHART_WIDTH - CHART_PADDING.left - CHART_PADDING.right
  const chartInnerHeight = CHART_HEIGHT - CHART_PADDING.top - CHART_PADDING.bottom
  const seasonIndex = new Map(seasons.map((season, index) => [season, index]))

  const xForSeason = (index: number) =>
    CHART_PADDING.left +
    (seasons.length === 1 ? chartInnerWidth / 2 : (index / (seasons.length - 1)) * chartInnerWidth)

  const yForScore = (score: number) =>
    CHART_PADDING.top + ((yAxis.max - score) / (yAxis.max - yAxis.min || 1)) * chartInnerHeight

  return {
    gridLines: yAxis.ticks.map((value) => ({
      value,
      y: yForScore(value),
      isZero: value === 0,
    })),
    xTicks: seasons.map((season, index) => ({
      season,
      label: startYearLabel(season),
      x: xForSeason(index),
    })),
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

function buildNiceYAxis(minScore: number, maxScore: number): {
  min: number
  max: number
  ticks: number[]
} {
  const paddedMin = Math.min(minScore, 0)
  const paddedMax = Math.max(maxScore, 0)
  const rawRange = paddedMax - paddedMin
  const step = Math.max(1, niceIntegerStep(rawRange <= 0 ? 1 : rawRange / 5))
  const min = Math.floor(paddedMin / step) * step
  const max = Math.ceil(paddedMax / step) * step
  const ticks: number[] = []

  for (let value = min; value <= max; value += step) {
    ticks.push(value)
  }

  if (!ticks.includes(0)) {
    ticks.push(0)
    ticks.sort((left, right) => left - right)
  }

  return { min, max, ticks }
}

function niceIntegerStep(value: number): number {
  const magnitude = 10 ** Math.floor(Math.log10(value))
  const normalized = value / magnitude

  if (normalized <= 1) {
    return magnitude
  }
  if (normalized <= 2) {
    return 2 * magnitude
  }
  if (normalized <= 5) {
    return 5 * magnitude
  }
  return 10 * magnitude
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

function startYearLabel(season: string): string {
  const [startYear] = season.split('-', 1)
  return startYear || season
}

function formatNumber(value: number, decimals: number): string {
  return value.toFixed(decimals)
}
