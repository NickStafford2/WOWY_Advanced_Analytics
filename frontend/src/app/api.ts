import {
  buildCachedLeaderboardParams,
  buildCustomQueryParams,
} from './query'
import type {
  CachedFilters,
  ErrorPayload,
  CustomFilters,
  LeaderboardPayload,
  MetricFilters,
  MetricId,
  MetricOptionsPayload,
  TeamOption,
} from './types'

export async function fetchMetricOptions(metric: MetricId): Promise<MetricOptionsPayload> {
  return (await fetchJson(`/api/metrics/${metric}/options`)) as MetricOptionsPayload
}

export async function fetchCachedLeaderboard(
  metric: MetricId,
  cachedFilters: CachedFilters,
  filters: MetricFilters,
): Promise<LeaderboardPayload> {
  const params = buildCachedLeaderboardParams(metric, cachedFilters, filters)
  return (await fetchJson(
    `/api/metrics/${metric}/cached-leaderboard?${params.toString()}`,
  )) as LeaderboardPayload
}

export async function fetchCustomLeaderboard(
  metric: MetricId,
  customFilters: CustomFilters,
  availableSeasons: string[],
  availableCustomTeams: TeamOption[],
): Promise<LeaderboardPayload> {
  const params = buildCustomQueryParams(
    metric,
    customFilters,
    availableSeasons,
    availableCustomTeams,
  )
  return (await fetchJson(
    `/api/metrics/${metric}/custom-query?${params.toString()}`,
  )) as LeaderboardPayload
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
