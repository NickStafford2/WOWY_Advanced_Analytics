export type ResultsTableRow = {
  rank: number
  player_id: number
  player_name: string
  span_average_value: number
  average_minutes: number | null
  total_minutes: number
  games_with: number
  games_without: number
  avg_margin_with: number | null
  avg_margin_without: number | null
  season_count: number
}

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
    <div className="results-table-panel">
      <div className="results-table-frame">
        <table className="results-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>Player</th>
              <th>{metricLabel}</th>
              <th>Seasons</th>
              <th>Avg Min</th>
              <th>Tot Min</th>
              {isWowyStyleMetric ? (
                <>
                  <th>With</th>
                  <th>Without</th>
                  <th>Avg With</th>
                  <th>Avg Without</th>
                </>
              ) : (
                <th>Games</th>
              )}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.player_id}>
                <td>{row.rank}</td>
                <td>{row.player_name}</td>
                <td>{formatNumber(row.span_average_value, 2)}</td>
                <td>{row.season_count}</td>
                <td>{formatNumber(row.average_minutes, 1)}</td>
                <td>{formatNumber(row.total_minutes, 1)}</td>
                {isWowyStyleMetric ? (
                  <>
                    <td>{row.games_with}</td>
                    <td>{row.games_without}</td>
                    <td>{formatNumber(row.avg_margin_with, 2)}</td>
                    <td>{formatNumber(row.avg_margin_without, 2)}</td>
                  </>
                ) : (
                  <td>{row.games_with}</td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="results-table-actions">
        <button
          type="button"
          className="results-export-button"
          onClick={() => {
            window.location.assign(exportUrl)
          }}
        >
          Export Full CSV
        </button>
      </div>
    </div>
  )
}

function formatNumber(value: number | null, decimals: number): string {
  return value === null ? '—' : value.toFixed(decimals)
}
