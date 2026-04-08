import type { LoadingPanelModel } from '../app/types'

type LoadingStatusProps = {
  model: LoadingPanelModel
}

export function LoadingStatus({ model }: LoadingStatusProps) {
  return (
    <section className="status-card status-card--loading" aria-live="polite">
      <div className="status-card__header">
        <div>
          <p className="panel-label">Live status</p>
          <h3>{model.title}</h3>
        </div>
        <strong>{model.progressPercent}%</strong>
      </div>

      <p className="status-card__summary">{model.summary}</p>

      <div
        className="status-progress"
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={model.progressPercent}
        aria-label={model.progressLabel}
      >
        <div className="status-progress__fill" style={{ width: `${model.progressPercent}%` }} />
      </div>

      <p className="status-card__label">{model.progressLabel}</p>

      <div className="status-card__phases">
        {model.phases.map((phase, index) => (
          <article
            key={phase.label}
            className={index === model.activePhaseIndex ? 'phase-card is-active' : 'phase-card'}
          >
            <strong>{phase.label}</strong>
            <p>{phase.detail}</p>
          </article>
        ))}
      </div>
    </section>
  )
}
