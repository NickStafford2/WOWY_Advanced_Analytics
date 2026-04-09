import { MathBlock } from './_MathBlock'

const PANEL_LABEL_CLASS_NAME =
  'm-0 text-xs font-bold tracking-[0.16em] uppercase text-[color:var(--accent-warm)]'
const SECTION_CLASS_NAME =
  'rounded-[28px] border border-[color:var(--panel-border)] bg-[var(--panel-soft-background)] p-6 shadow-[var(--panel-shadow)] max-sm:rounded-[22px] max-sm:p-[18px]'
const INTRO_COPY_CLASS_NAME =
  'm-0 max-w-[68ch] text-[1.02rem] leading-[1.65] text-[color:var(--text-muted)]'
const GRID_CLASS_NAME = 'mt-5 grid grid-cols-2 gap-4 max-[1120px]:grid-cols-1'
const CARD_CLASS_NAME =
  'rounded-3xl border border-[color:var(--panel-border-soft)] bg-[var(--panel-card-background)] p-5'
const BODY_COPY_CLASS_NAME = 'm-0 leading-[1.65] text-[color:var(--text-muted)]'
const SUBTITLE_CLASS_NAME = 'mt-2 mb-3 font-bold text-[color:var(--text-soft)]'
const MATH_SECTION_CLASS_NAME =
  'mt-5 border-t border-[color:var(--panel-border-soft)] pt-5'

const MODEL_EQUATION = String.raw`\begin{aligned}
\operatorname{predicted\_margin}_i &= \beta_0 \\
&\quad + \beta_{\text{home}} \cdot \operatorname{home\_sign}_i \\
&\quad + \sum_p x_{i,p} \beta_p \\
&\quad + \gamma_{\operatorname{team}(i)} \\
&\quad + \delta_{\operatorname{opponent}(i)}
\end{aligned}`

const FIT_EQUATION = String.raw`\begin{aligned}
\hat{\beta} = \arg\min_{\beta}\; &\sum_i \left(\operatorname{margin}_i - \operatorname{predicted\_margin}_i\right)^2 \\
&+ \operatorname{ridge\_alpha} \sum_p \beta_p^2 \\
&+ \operatorname{ridge\_alpha} \sum_t \gamma_t^2 \\
&+ \operatorname{ridge\_alpha} \sum_t \delta_t^2
\end{aligned}`

const MODEL_EQUATION_WHERE = [
  { label: 'i', description: 'indexes one filtered game observation.' },
  {
    label: String.raw`\operatorname{predicted\_margin}_i`,
    description: 'the model-predicted team margin for observation i.',
  },
  { label: '\\beta_0', description: 'the intercept term.' },
  {
    label: String.raw`\beta_{\text{home}} \cdot \operatorname{home\_sign}_i`,
    description: 'the home-court adjustment for observation i.',
  },
  { label: 'x_{i,p}', description: 'player p\'s feature weight in observation i.' },
  { label: '\\beta_p', description: 'the fitted coefficient for player p.' },
  {
    label: String.raw`\gamma_{\operatorname{team}(i)}`,
    description: 'the team-season effect for the team in observation i.',
  },
  {
    label: String.raw`\delta_{\operatorname{opponent}(i)}`,
    description: 'the opponent team-season effect in observation i.',
  },
] as const

const FIT_EQUATION_WHERE = [
  { label: '\\hat{\\beta}', description: 'the full set of fitted coefficients.' },
  {
    label: String.raw`\operatorname{margin}_i`,
    description: 'the observed team margin in observation i.',
  },
  {
    label: String.raw`\operatorname{predicted\_margin}_i`,
    description: 'the model prediction for observation i.',
  },
  { label: 'p', description: 'ranges over player terms.' },
  { label: 't', description: 'ranges over team-season and opponent terms.' },
  {
    label: String.raw`\operatorname{ridge\_alpha}`,
    description: 'the shrinkage strength applied to the coefficients.',
  },
] as const

export function AboutRawr() {
  return (
    <section className={SECTION_CLASS_NAME}>
      <div className="flex flex-col gap-[14px]">
        <span className={PANEL_LABEL_CLASS_NAME}>Current analytic</span>
        <h1 className="text-[clamp(2rem,4vw,3rem)] leading-none">
          RAWR estimates player impact with a ridge regression over game-level results.
        </h1>
        <p className={INTRO_COPY_CLASS_NAME}>
          Use RAWR when you want a model-based estimate of player value instead of a direct
          with-or-without split. The query takes the selected seasons and team scope, builds a
          game-level sample, and estimates how strongly each player is associated with team
          performance after the model accounts for the rest of the lineup context.
        </p>
      </div>

      <div className={GRID_CLASS_NAME}>
        <article className={CARD_CLASS_NAME}>
          <h2 className="m-0 text-[1.4rem] leading-[1.15]">Non-Obvious Filters</h2>
          <p className={SUBTITLE_CLASS_NAME}>What changes the estimate</p>
          <p className={BODY_COPY_CLASS_NAME}>
            <strong>Ridge alpha</strong> controls how aggressively the regression shrinks player
            estimates toward zero. Higher values make the model more conservative and reduce noisy
            extremes. Lower values let the fitted values react more strongly to the data.
          </p>
          <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
            <strong>Min games</strong> removes players with too little game coverage before the
            leaderboard is returned. <strong>Min average minutes</strong> and{' '}
            <strong>Min total minutes</strong> are often the more important filters, because they
            stop the board from being dominated by players whose estimate comes from a thin minute
            sample.
          </p>
        </article>

        <article className={CARD_CLASS_NAME}>
          <h2 className="m-0 text-[1.4rem] leading-[1.15]">How To Read Results</h2>
          <p className={SUBTITLE_CLASS_NAME}>Interpretation</p>
          <p className={BODY_COPY_CLASS_NAME}>
            Read RAWR as a comparative estimate inside the exact query you ran. If you change the
            season span, teams, or thresholds, you are changing the evidence the model is allowed
            to learn from.
          </p>
          <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
            Start with players who remain strong under stricter minute and game filters. Those are
            usually the more credible signals. Use the export when you want to compare multiple
            query settings side by side rather than reading a single board in isolation.
          </p>
        </article>
      </div>

      <section className={MATH_SECTION_CLASS_NAME}>
        <h2 className="m-0 text-[1.4rem] leading-[1.15]">Math</h2>
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          RAWR asks a simple question first: what set of player values best explains the game
          margins in the filtered sample? Instead of comparing with and without buckets directly,
          it fits one model across all selected games at once.
        </p>
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          The general idea is regression. Regression fits coefficients so predicted outcomes stay as
          close as possible to observed outcomes. Here the observed outcome is game margin, and the
          model uses player terms plus a few context terms to explain it.
        </p>
        <MathBlock equation={MODEL_EQUATION} whereItems={MODEL_EQUATION_WHERE} />
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          The fit uses ridge regression, which means ordinary squared-error fitting plus a penalty
          that pulls coefficients back toward zero. That penalty is what keeps the model from
          chasing every noisy fluctuation in the sample.
        </p>
        <MathBlock equation={FIT_EQUATION} whereItems={FIT_EQUATION_WHERE} />
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          Bigger <strong>Ridge alpha</strong> means more shrinkage toward zero and therefore more
          conservative player estimates. Smaller values let the model follow the sample more
          aggressively. The leaderboard value for a player is that player&apos;s fitted coefficient.
        </p>
      </section>
    </section>
  )
}
