import { renderToString } from 'katex'

type MathWhereItem = {
  label: string
  description: string
}

type MathBlockProps = {
  equation: string
  whereItems?: readonly MathWhereItem[]
}

export function MathBlock({ equation, whereItems = [] }: MathBlockProps) {
  const renderedEquation = renderToString(equation, {
    displayMode: true,
    output: 'html',
    throwOnError: false,
  })

  return (
    <div className="katex-block mt-[14px] overflow-x-auto rounded-[18px] border border-[color:var(--panel-border-soft)] [background:var(--chart-frame-background)] px-[18px] py-4">
      <div dangerouslySetInnerHTML={{ __html: renderedEquation }} />
      {whereItems.length > 0 ? (
        <div className="mt-3 border-t border-[color:var(--panel-border-soft)] pt-3">
          <p className="m-0 text-[0.95rem] italic text-[color:var(--text-secondary)]">where</p>
          <dl className="mt-2 grid gap-1.5">
            {whereItems.map((item) => (
              <div
                key={item.label}
                className="grid gap-1 min-[860px]:grid-cols-[minmax(0,13rem)_minmax(0,1fr)] min-[860px]:gap-3"
              >
                <dt
                  className="text-[color:var(--text-secondary)]"
                  dangerouslySetInnerHTML={{
                    __html: renderToString(item.label, {
                      displayMode: false,
                      output: 'html',
                      throwOnError: false,
                    }),
                  }}
                />
                <dd className="m-0 leading-[1.55] text-[color:var(--text-muted)]">
                  {item.description}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      ) : null}
    </div>
  )
}
