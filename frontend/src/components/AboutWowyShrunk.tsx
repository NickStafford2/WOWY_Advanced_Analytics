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

const SHRINKAGE_EQUATION = String.raw`\begin{aligned}
\operatorname{wowy\_score} &= \operatorname{avg\_with} - \operatorname{avg\_without} \\
\operatorname{effective\_games} &= \frac{2 \cdot \operatorname{games\_with} \cdot \operatorname{games\_without}}{\operatorname{games\_with} + \operatorname{games\_without}} \\
\operatorname{shrinkage\_factor} &= \frac{\operatorname{effective\_games}}{\operatorname{effective\_games} + \operatorname{prior\_games}} \\
\operatorname{shrunk\_score} &= \operatorname{wowy\_score} \cdot \operatorname{shrinkage\_factor}
\end{aligned}`

export function AboutWowyShrunk() {
  return (
    <section className={SECTION_CLASS_NAME}>
      <div className="flex flex-col gap-[14px]">
        <span className={PANEL_LABEL_CLASS_NAME}>Current analytic</span>
        <h1 className="text-[clamp(2rem,4vw,3rem)] leading-none">
          WOWY Shrinkage starts from WOWY, then pulls unstable estimates toward a safer middle.
        </h1>
        <p className={INTRO_COPY_CLASS_NAME}>
          Use WOWY Shrinkage when plain WOWY is directionally useful but too volatile. The query
          still depends on with-and-without samples, but it softens extreme results when the data
          behind them is thin.
        </p>
      </div>

      <div className={GRID_CLASS_NAME}>
        <article className={CARD_CLASS_NAME}>
          <h2 className="m-0 text-[1.4rem] leading-[1.15]">How It Compares To WOWY</h2>
          <p className={SUBTITLE_CLASS_NAME}>Same idea, steadier output</p>
          <p className={BODY_COPY_CLASS_NAME}>
            Plain WOWY reports the direct with-and-without difference as observed in the query
            sample. WOWY Shrinkage uses that same evidence but discounts the most fragile edges of
            the board.
          </p>
          <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
            If a player looks extreme in WOWY but ordinary here, that is usually a sign that the
            WOWY result was driven by limited evidence. If a player remains strong in both views,
            the signal is more likely to be robust.
          </p>
        </article>

        <article className={CARD_CLASS_NAME}>
          <h2 className="m-0 text-[1.4rem] leading-[1.15]">Non-Obvious Filters</h2>
          <p className={SUBTITLE_CLASS_NAME}>Still driven by the sample</p>
          <p className={BODY_COPY_CLASS_NAME}>
            <strong>Min games with</strong> and <strong>Min games without</strong> still matter,
            even though shrinkage stabilizes the output. Shrinkage is not a substitute for sample
            discipline. It only makes thin samples less explosive.
          </p>
          <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
            <strong>Min average minutes</strong>, <strong>Min total minutes</strong>, and custom
            team scope all still change the underlying evidence. The most useful workflow is to
            compare this board against plain WOWY under the same filters and see where the ranking
            meaningfully tightens.
          </p>
        </article>
      </div>

      <section className={MATH_SECTION_CLASS_NAME}>
        <h2 className="m-0 text-[1.4rem] leading-[1.15]">Math</h2>
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          WOWY Shrinkage still begins with the plain WOWY question: compare team margin with the
          player against team margin without the player. The difference is that the final score is
          pulled back toward zero when the evidence is thin.
        </p>
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          The general idea is shrinkage. Shrinkage leaves strong, well-supported estimates closer
          to their observed value and compresses weaker estimates toward a safer middle. Here that
          compression depends on the harmonic mean of the with and without game counts.
        </p>
        <MathBlock equation={SHRINKAGE_EQUATION} />
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          In the current backend, <code>prior_games</code> defaults to <code>10</code>. If the with
          and without samples are both large, the shrinkage factor moves toward <code>1</code> and
          the result stays close to plain WOWY. If either side is small, the factor drops and the
          score is pulled toward zero.
        </p>
      </section>
    </section>
  )
}
