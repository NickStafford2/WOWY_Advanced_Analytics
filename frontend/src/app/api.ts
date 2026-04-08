import { buildLeaderboardParams } from './query'
import type {
  ErrorPayload,
  LeaderboardFilters,
  LeaderboardPayload,
  MetricId,
  MetricOptionsPayload,
  TeamOption,
} from './types'

export async function fetchLeaderboard(
  metric: MetricId,
  filters: LeaderboardFilters,
  availableSeasons: string[],
  availableTeams: TeamOption[],
  signal?: AbortSignal,
): Promise<LeaderboardPayload> {
  const params = buildLeaderboardParams(metric, filters, availableSeasons, availableTeams)
  return (
    await _fetchJson(`/api/metrics/${metric}/leaderboard?${params.toString()}`, signal)
  ) as LeaderboardPayload
}

export async function fetchMetricOptions(
  metric: MetricId,
  signal?: AbortSignal,
): Promise<MetricOptionsPayload> {
  return (await _fetchJson(`/api/metrics/${metric}/options`, signal)) as MetricOptionsPayload
}

async function _fetchJson(url: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(url, { signal })
  const contentType = response.headers.get('content-type') ?? ''
  const bodyText = await response.text()

  if (!contentType.includes('application/json')) {
    if (response.status >= 500 || bodyText.trimStart().startsWith('<')) {
      throw new Error(
        'The web API is unavailable or returned HTML. Start the backend with `poetry run wowy-web`.',
      )
    }
    throw new Error(bodyText || 'Request failed')
  }

  const payload = JSON.parse(bodyText) as unknown
  if (!response.ok) {
    const errorPayload = payload as ErrorPayload
    throw new Error(errorPayload.error ?? `Request failed (${response.status})`)
  }

  return payload
}
