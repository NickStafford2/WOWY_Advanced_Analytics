import { metricOptions } from '../app/metric'
import type { MetricId, ThemeMode } from '../app/metricTypes'

const PANEL_LABEL_CLASS_NAME =
  'm-0 text-xs font-bold tracking-[0.16em] uppercase text-[color:var(--accent-warm)]'
const META_BADGE_CLASS_NAME =
  'rounded-full bg-[var(--meta-background)] px-3 py-[9px] text-[0.88rem] font-semibold text-[color:var(--meta-text)]'
const CONTROL_BUTTON_CLASS_NAME =
  'min-h-[42px] cursor-pointer rounded-[14px] border border-[color:var(--control-border)] bg-[var(--input-background)] px-[14px] font-bold text-[color:var(--text-secondary)] transition-colors disabled:cursor-not-allowed disabled:opacity-60'
const ACTIVE_SEGMENT_CLASS_NAME =
  'border-[color:var(--accent-border)] [background:var(--accent-gradient-strong)] text-[color:var(--text-inverse)]'

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
    <header className="grid gap-6 rounded-[28px] border border-[color:var(--panel-border)] [background:var(--hero-background)] p-7 shadow-[var(--panel-shadow)] min-[1121px]:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.95fr)] max-sm:rounded-[22px] max-sm:p-[18px]">
      <div>
        <p className={PANEL_LABEL_CLASS_NAME}>RAWR Analytics</p>
        <h1 className="mt-3 mb-[10px] text-[clamp(2.75rem,6vw,5rem)] leading-[0.92] tracking-[-0.05em]">
          {metricLabel}
        </h1>
        <p className="m-0 max-w-[68ch] text-[1.02rem] leading-[1.65] text-[color:var(--text-muted)]">
          {metricDescription}
        </p>
        <div className="mt-[18px] flex flex-wrap gap-[10px]" aria-label="Dataset summary">
          <span className={META_BADGE_CLASS_NAME}>{seasonCount || 'No'} seasons loaded</span>
          <span className={META_BADGE_CLASS_NAME}>{teamCount || 'No'} teams in scope</span>
        </div>
      </div>

      <div className="flex flex-col gap-[14px] min-[1121px]:max-w-[30rem] min-[1121px]:justify-self-end max-[1120px]:order-first">
        <section className="rounded-[20px] border border-[color:var(--panel-border-soft)] bg-[var(--panel-card-background)] p-4">
          <p className={PANEL_LABEL_CLASS_NAME}>Metric</p>
          <div
            className="mt-3 flex flex-wrap gap-[10px] max-sm:flex-col"
            role="tablist"
            aria-label="Metric selector"
          >
            {metricChoices.map((option) => (
              <button
                key={option.id}
                type="button"
                className={`${CONTROL_BUTTON_CLASS_NAME} flex-1 basis-[140px] ${metric === option.id ? ACTIVE_SEGMENT_CLASS_NAME : ''}`}
                onClick={() => onMetricChange(option.id)}
              >
                {option.label}
              </button>
            ))}
          </div>
        </section>

        <button
          type="button"
          className={`${CONTROL_BUTTON_CLASS_NAME} self-start`}
          onClick={onThemeToggle}
        >
          {theme === 'dark' ? 'Light mode' : 'Dark mode'}
        </button>
      </div>
    </header>
  )
}
