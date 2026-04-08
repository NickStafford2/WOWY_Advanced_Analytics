import type { AppMode, MetricId, ThemeMode } from '../app/types'

const METRIC_OPTIONS: { id: MetricId; label: string }[] = [
  { id: 'rawr', label: 'RAWR' },
  { id: 'wowy_shrunk', label: 'WOWY Shrinkage' },
  { id: 'wowy', label: 'WOWY' },
]

const MODE_OPTIONS: { id: AppMode; label: string }[] = [
  { id: 'cached', label: 'All-Time Leaders' },
  { id: 'custom', label: 'Custom Query' },
]

type AppHeaderProps = {
  metric: MetricId
  mode: AppMode
  metricLabel: string
  metricDescription: string
  seasonCount: number
  teamCount: number
  theme: ThemeMode
  onMetricChange: (metric: MetricId) => void
  onModeChange: (mode: AppMode) => void
  onThemeToggle: () => void
}

export function AppHeader({
  metric,
  mode,
  metricLabel,
  metricDescription,
  seasonCount,
  teamCount,
  theme,
  onMetricChange,
  onModeChange,
  onThemeToggle,
}: AppHeaderProps) {
  return (
    <header className="app-header">
      <div className="app-header__intro">
        <p className="eyebrow">RAWR Analytics</p>
        <h1>{metricLabel}</h1>
        <p className="lede">{metricDescription}</p>
        <div className="app-header__meta" aria-label="Dataset summary">
          <span>{seasonCount || 'No'} seasons loaded</span>
          <span>{teamCount || 'No'} teams in scope</span>
          <span>{mode === 'cached' ? 'Cached leaderboard' : 'Live custom query'}</span>
        </div>
      </div>

      <div className="app-header__controls">
        <section className="header-card">
          <p className="panel-label">Metric</p>
          <div className="segmented-control" role="tablist" aria-label="Metric selector">
            {METRIC_OPTIONS.map((option) => (
              <button
                key={option.id}
                type="button"
                className={metric === option.id ? 'segment is-active' : 'segment'}
                onClick={() => onMetricChange(option.id)}
              >
                {option.label}
              </button>
            ))}
          </div>
        </section>

        <section className="header-card">
          <p className="panel-label">View</p>
          <div className="segmented-control" role="tablist" aria-label="Query mode">
            {MODE_OPTIONS.map((option) => (
              <button
                key={option.id}
                type="button"
                className={mode === option.id ? 'segment is-active' : 'segment'}
                onClick={() => onModeChange(option.id)}
              >
                {option.label}
              </button>
            ))}
          </div>
        </section>

        <button type="button" className="theme-toggle-button" onClick={onThemeToggle}>
          {theme === 'dark' ? 'Light mode' : 'Dark mode'}
        </button>
      </div>
    </header>
  )
}
