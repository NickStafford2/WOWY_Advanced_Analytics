import { useMemo, useState } from 'react'
import type { SpanSeries } from '../app/leaderboardApiTypes'

const CHART_WIDTH = 920
const CHART_HEIGHT = 420
const CHART_PADDING = { top: 24, right: 28, bottom: 48, left: 60 }
const MAX_X_AXIS_LABELS = 8
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
    <div className="mt-[18px] grid gap-[14px] min-[1121px]:grid-cols-[minmax(0,1fr)_240px]">
      <div className="overflow-auto rounded-3xl border border-[color:var(--panel-border-soft)] [background:var(--chart-frame-background)]">
        <svg
          className="block min-w-[840px] w-full"
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
                className={line.isZero ? 'stroke-[#2a9d8f] stroke-[1.4]' : 'stroke-[var(--grid-line)] stroke-1'}
              />
              <text
                x={18}
                y={line.y + 4}
                className="fill-[var(--text-faint)] text-[0.8rem]"
              >
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
                className="stroke-[var(--grid-line)] stroke-1 [stroke-dasharray:5_7]"
              />
              <text
                x={tick.x}
                y={CHART_HEIGHT - 16}
                textAnchor="middle"
                className="fill-[var(--text-faint)] text-[0.8rem]"
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
              <g key={entry.player_id} className={isDimmed ? 'opacity-90' : undefined}>
                {entry.segments.map((segment, segmentIndex) => (
                  <polyline
                    key={`${entry.player_id}-${segmentIndex}`}
                    className={isActive ? 'drop-shadow-[0_0_6px_var(--status-track)] transition-all duration-150' : 'transition-all duration-150'}
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
                      className={isActive ? 'cursor-pointer drop-shadow-[0_0_6px_var(--status-track)] transition-all duration-150' : 'cursor-pointer transition-all duration-150'}
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
      <aside
        className="rounded-3xl border border-[color:var(--panel-border-soft)] [background:var(--legend-background)] p-[14px]"
        aria-label="Chart legend"
      >
        <p className="m-0 text-xs font-bold tracking-[0.16em] uppercase text-[color:var(--accent-warm)]">
          Legend
        </p>
        <ul className="mt-[10px] flex list-none flex-col gap-2 p-0">
          {chartModel.series.map((entry, index) => {
            const isActive = activePlayerId === entry.player_id
            const isDimmed = activePlayerId !== null && !isActive
            return (
              <li
                key={`legend-${entry.player_id}`}
                className={`grid cursor-pointer grid-cols-[10px_minmax(0,1fr)] items-center gap-[10px] transition-all duration-150 ${isDimmed ? 'opacity-[0.58]' : isActive ? '-translate-x-0.5' : ''}`}
                onMouseEnter={() => setActivePlayerId(entry.player_id)}
                onMouseLeave={() => setActivePlayerId(null)}
              >
                <span
                  className="h-[10px] w-[10px] rounded-full transition-all duration-150"
                  style={{
                    backgroundColor: isDimmed
                      ? 'rgba(120, 128, 136, 0.45)'
                      : SERIES_COLORS[index % SERIES_COLORS.length],
                  }}
                  aria-hidden="true"
                />
                <div className="flex justify-between gap-3 max-sm:flex-col max-sm:items-start">
                  <strong
                    className={isDimmed
                      ? 'text-[0.92rem] leading-[1.25] text-[color:var(--text-faint)] transition-colors duration-150'
                      : 'text-[0.92rem] leading-[1.25] transition-colors duration-150'}
                  >
                    {entry.player_name}
                  </strong>
                  <small
                    className={isDimmed
                      ? 'leading-[1.25] text-[color:var(--text-faint)] transition-colors duration-150'
                      : 'leading-[1.25] text-[color:var(--text-muted)] transition-colors duration-150'}
                  >
                    {formatNumber(entry.span_average_value, 2)}
                  </small>
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
  const tickIndexes = visibleTickIndexes(seasons.length)

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
    xTicks: tickIndexes.map((index) => ({
      season: seasons[index] ?? '',
      label: startYearLabel(seasons[index] ?? ''),
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

function visibleTickIndexes(seasonCount: number): number[] {
  if (seasonCount <= MAX_X_AXIS_LABELS) {
    return Array.from({ length: seasonCount }, (_, index) => index)
  }

  const tickIndexes: number[] = []
  const step = Math.ceil((seasonCount - 1) / (MAX_X_AXIS_LABELS - 1))

  for (let index = 0; index < seasonCount; index += step) {
    tickIndexes.push(index)
  }

  const lastIndex = seasonCount - 1
  if (tickIndexes.at(-1) !== lastIndex) {
    tickIndexes.push(lastIndex)
  }

  return tickIndexes
}

function startYearLabel(season: string): string {
  const [startYear] = season.split('-', 1)
  return startYear || season
}

function formatNumber(value: number, decimals: number): string {
  return value.toFixed(decimals)
}
