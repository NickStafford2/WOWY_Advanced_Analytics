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
const EQUATION_CLASS_NAME =
  'mt-[14px] overflow-x-auto rounded-[18px] border border-[color:var(--panel-border-soft)] [background:var(--chart-frame-background)] px-[18px] py-4 text-[0.92rem] leading-[1.6] whitespace-pre-wrap text-[color:var(--text-primary)]'

export function AboutWowy() {
  return (
    <section className={SECTION_CLASS_NAME}>
      <div className="flex flex-col gap-[14px]">
        <span className={PANEL_LABEL_CLASS_NAME}>Current analytic</span>
        <h1 className="text-[clamp(2rem,4vw,3rem)] leading-none">
          WOWY compares team results with a player in the sample and without them.
        </h1>
        <p className={INTRO_COPY_CLASS_NAME}>
          Use WOWY when you want the direct with-or-without view. The query does not try to fit a
          model. It measures how team margin changes across the filtered games when the player is
          present versus absent.
        </p>
      </div>

      <div className={GRID_CLASS_NAME}>
        <article className={CARD_CLASS_NAME}>
          <h2 className="m-0 text-[1.4rem] leading-[1.15]">Non-Obvious Filters</h2>
          <p className={SUBTITLE_CLASS_NAME}>Sample quality matters</p>
          <p className={BODY_COPY_CLASS_NAME}>
            <strong>Min games with</strong> and <strong>Min games without</strong> are the key WOWY
            controls. They force both sides of the comparison to have enough evidence. If one side
            is too small, the apparent impact can look dramatic without being trustworthy.
          </p>
          <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
            <strong>Min average minutes</strong> and <strong>Min total minutes</strong> cut out
            players whose rows are technically valid but not useful. Team selection also matters:
            custom team scope changes which games are allowed into both the with and without
            buckets.
          </p>
        </article>

        <article className={CARD_CLASS_NAME}>
          <h2 className="m-0 text-[1.4rem] leading-[1.15]">How To Read Results</h2>
          <p className={SUBTITLE_CLASS_NAME}>Practical interpretation</p>
          <p className={BODY_COPY_CLASS_NAME}>
            Start by checking whether both sides of the split are well populated. A player with a
            large difference between <strong>Avg With</strong> and <strong>Avg Without</strong> is
            only interesting if the game counts and minutes support it.
          </p>
          <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
            WOWY is best for asking concrete sample questions, such as whether a player&apos;s teams
            consistently performed better or worse when he appeared in the filtered game set. It is
            less useful when you want the model to smooth out noisy context for you.
          </p>
        </article>
      </div>

      <section className={MATH_SECTION_CLASS_NAME}>
        <h2 className="m-0 text-[1.4rem] leading-[1.15]">Math</h2>
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          WOWY starts from the most direct version of the question: how did the team perform in
          the filtered games when this player appeared, and how did it perform when he did not?
        </p>
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          Mathematically, that is just the difference between two sample means. The table exposes
          both sides of the split directly through <strong>With</strong>, <strong>Without</strong>,
          <strong>Avg With</strong>, and <strong>Avg Without</strong>.
        </p>
        <pre className={EQUATION_CLASS_NAME}>
{`avg_with =
  (1 / games_with) * sum(margin_g for games g where player appears)

avg_without =
  (1 / games_without) * sum(margin_g for games g where player does not appear)

wowy_score = avg_with - avg_without`}
        </pre>
        <p className="mt-3 leading-[1.65] text-[color:var(--text-muted)]">
          That is why <strong>Min games with</strong> and <strong>Min games without</strong> matter
          so much. WOWY is only as stable as the two sample means it is subtracting.
        </p>
      </section>
    </section>
  )
}
