import { buildLeaderboardParams } from './query'
import type {
  ErrorPayload,
  LeaderboardFilters,
  LeaderboardPayload,
  MetricId,
  MetricOptionsPayload,
  TeamOption,
} from './types'

export async function fetchMetricOptions(metric: MetricId): Promise<MetricOptionsPayload> {
  return (await fetchJson(`/api/metrics/${metric}/options`)) as MetricOptionsPayload
}

export async function fetchLeaderboard(
  metric: MetricId,
  filters: LeaderboardFilters,
  availableSeasons: string[],
  availableTeams: TeamOption[],
): Promise<LeaderboardPayload> {
  const params = buildLeaderboardParams(metric, filters, availableSeasons, availableTeams)
  return (await fetchJson(`/api/metrics/${metric}/leaderboard?${params.toString()}`)) as LeaderboardPayload
}

async function fetchJson(url: string): Promise<unknown> {
  const response = await fetch(url)
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
