import type { LoadingPanelModel } from '../app/loadingTypes'

const PANEL_LABEL_CLASS_NAME =
  'm-0 text-xs font-bold tracking-[0.16em] uppercase text-[color:var(--accent-warm)]'

type LoadingStatusProps = {
  model: LoadingPanelModel
}

export function LoadingStatus({ model }: LoadingStatusProps) {
  return (
    <section
      className="mt-[18px] rounded-[18px] border border-[color:var(--panel-border-soft)] [background:var(--status-loading-background)] p-[18px] text-[color:var(--text-secondary)]"
      aria-live="polite"
    >
      <div className="flex items-end justify-between gap-4">
        <div>
          <p className={PANEL_LABEL_CLASS_NAME}>Live status</p>
          <h3 className="mt-2 text-[1.4rem] leading-[1.1]">{model.title}</h3>
        </div>
        <strong className="text-[1.8rem] leading-none">{model.progressPercent}%</strong>
      </div>

      <p className="mt-[14px] m-0 leading-[1.55] text-[color:var(--text-muted)]">{model.summary}</p>

      <div
        className="mt-4 h-[14px] overflow-hidden rounded-full bg-[var(--status-track)]"
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={model.progressPercent}
        aria-label={model.progressLabel}
      >
        <div
          className="h-full rounded-full bg-[linear-gradient(90deg,#d65f37,#1e8d88)] transition-[width] duration-200 ease-linear"
          style={{ width: `${model.progressPercent}%` }}
        />
      </div>

      <p className="mt-[10px] m-0 text-[0.9rem] text-[color:var(--text-muted)]">
        {model.progressLabel}
      </p>

      <div className="mt-4 grid grid-cols-3 gap-3 max-[1120px]:grid-cols-1">
        {model.phases.map((phase, index) => (
          <article
            key={phase.label}
            className={`rounded-2xl border border-[color:var(--panel-border-soft)] p-[14px] transition-[opacity,transform,border-color] duration-150 ${index === model.activePhaseIndex ? 'translate-y-[-1px] border-[color:var(--accent-border)] bg-[var(--status-phase-background-active)] opacity-100' : 'bg-[var(--status-phase-background)] opacity-65'}`}
          >
            <strong className="mb-1.5 block">{phase.label}</strong>
            <p className="m-0 leading-[1.55]">{phase.detail}</p>
          </article>
        ))}
      </div>
      {model.debug != null && (
        <div className="mt-4 rounded-xl border border-[color:var(--panel-border-soft)] bg-black/20 p-3 text-[0.75rem] font-mono text-[color:var(--text-muted)]">
          <div>phase: {model.debug.phase}</div>
          <div>
            step: {model.debug.current} / {model.debug.total}
          </div>
          <div>detail: {model.summary}</div>
        </div>
      )}
    </section>
  )
}
