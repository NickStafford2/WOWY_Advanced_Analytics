type LogLevel = 'log' | 'warn' | 'error'

export function debugLeaderboard(
  scope: string,
  message: string,
  details?: unknown,
  level: LogLevel = 'log',
): void {
  const prefix = `[Leaderboard ${scope}] ${message}`

  if (details === undefined) {
    console[level](prefix)
    return
  }

  console[level](prefix, details)
}
