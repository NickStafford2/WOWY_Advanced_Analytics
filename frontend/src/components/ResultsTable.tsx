import type { ResultsTableRow } from '../app/leaderboardApiTypes'

type ResultsTableProps = {
  metricLabel: string
  exportUrl: string
  rows: ResultsTableRow[]
  isWowyStyleMetric: boolean
}

export function ResultsTable({
  metricLabel,
  exportUrl,
  rows,
  isWowyStyleMetric,
}: ResultsTableProps) {
  return (
    <div className="mt-[22px]">
      <div className="overflow-auto rounded-3xl border border-[color:var(--panel-border-soft)] [background:var(--table-background)]">
        <table className="w-full min-w-[920px] border-collapse">
          <thead className="sticky top-0">
            <tr>
              <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">Rank</th>
              <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-left text-[0.8rem] tracking-[0.08em] uppercase">Player</th>
              <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">{metricLabel}</th>
              <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">Seasons</th>
              <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">Avg Min</th>
              <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">Tot Min</th>
              {isWowyStyleMetric ? (
                <>
                  <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">With</th>
                  <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">Without</th>
                  <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">Avg With</th>
                  <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">Avg Without</th>
                </>
              ) : (
                <th className="sticky top-0 border-b border-[color:var(--panel-border-soft)] bg-[var(--table-head-background)] px-4 py-[14px] text-right text-[0.8rem] tracking-[0.08em] uppercase">Games</th>
              )}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr
                key={row.player_id}
                className={index % 2 === 1 ? 'bg-[var(--table-alt-row)]' : undefined}
              >
                <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{row.rank}</td>
                <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-left whitespace-nowrap">{row.player_name}</td>
                <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{formatNumber(row.span_average_value, 2)}</td>
                <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{row.season_count}</td>
                <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{formatNumber(row.average_minutes, 1)}</td>
                <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{formatNumber(row.total_minutes, 1)}</td>
                {isWowyStyleMetric ? (
                  <>
                    <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{formatInteger(row.games_with)}</td>
                    <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{formatInteger(row.games_without)}</td>
                    <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{formatNumber(row.avg_margin_with, 2)}</td>
                    <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{formatNumber(row.avg_margin_without, 2)}</td>
                  </>
                ) : (
                  <td className="border-b border-[color:var(--panel-border-soft)] px-4 py-[14px] text-right whitespace-nowrap">{formatInteger(row.games)}</td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="mt-3 flex justify-end">
        <a
          className="inline-flex min-h-[42px] items-center justify-center rounded-[14px] border border-[color:var(--control-border)] bg-[var(--input-background)] px-4 font-bold text-[color:var(--text-secondary)] no-underline"
          href={exportUrl}
        >
          Export Full CSV
        </a>
      </div>
    </div>
  )
}

function formatNumber(value: number | null | undefined, decimals: number): string {
  return value == null ? '—' : value.toFixed(decimals)
}

function formatInteger(value: number | null | undefined): string {
  return value == null ? '—' : String(value)
}
