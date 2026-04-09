import { renderToString } from 'katex'

type MathBlockProps = {
  equation: string
}

export function MathBlock({ equation }: MathBlockProps) {
  const renderedEquation = renderToString(equation, {
    displayMode: true,
    throwOnError: false,
  })

  return (
    <div
      className="katex-block mt-[14px] overflow-x-auto rounded-[18px] border border-[color:var(--panel-border-soft)] [background:var(--chart-frame-background)] px-[18px] py-4"
      dangerouslySetInnerHTML={{ __html: renderedEquation }}
    />
  )
}
