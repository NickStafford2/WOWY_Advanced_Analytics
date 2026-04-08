import { metricOptions } from '../app/metric'
import type { MetricId, ThemeMode } from '../app/types'

type AppHeaderProps = {
  metric: MetricId
  metricLabel: string
  metricDescription: string
  seasonCount: number
  teamCount: number
  theme: ThemeMode
  onMetricChange: (metric: MetricId) => void
  onThemeToggle: () => void
}

export function AppHeader({
  metric,
  metricLabel,
  metricDescription,
  seasonCount,
  teamCount,
  theme,
  onMetricChange,
  onThemeToggle,
}: AppHeaderProps) {
  const metricChoices = metricOptions()

  return (
    <header className="app-header">
      <div className="app-header__intro">
        <p className="eyebrow">RAWR Analytics</p>
        <h1>{metricLabel}</h1>
        <p className="lede">{metricDescription}</p>
        <div className="app-header__meta" aria-label="Dataset summary">
          <span>{seasonCount || 'No'} seasons loaded</span>
          <span>{teamCount || 'No'} teams in scope</span>
          <span>Unified leaderboard query</span>
        </div>
      </div>

      <div className="app-header__controls">
        <section className="header-card">
          <p className="panel-label">Metric</p>
          <div className="segmented-control" role="tablist" aria-label="Metric selector">
            {metricChoices.map((option) => (
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
          <p className="panel-label">Execution</p>
          <p className="sidebar-note">
            Each request hits one leaderboard endpoint. The backend returns cached results when the
            scope is already materialized and computes live results otherwise.
          </p>
        </section>

        <button type="button" className="theme-toggle-button" onClick={onThemeToggle}>
          {theme === 'dark' ? 'Light mode' : 'Dark mode'}
        </button>
      </div>
    </header>
  )
}
