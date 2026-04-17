import { useEffect, useRef, useState, type Dispatch, type MutableRefObject, type SetStateAction } from 'react'
import { fetchMetricOptions } from './api'
import {
  defaultLeaderboardFilters,
  initializeLeaderboardFiltersWithOptions,
  syncScopedLeaderboardFiltersWithOptions,
} from './leaderboardFilters'
import type { TeamOption } from './leaderboardApiTypes'
import {
  buildMetricOptionsParamsForSeasonSpan,
  buildMetricOptionsParamsForTeams,
} from './leaderboardParams'
import type { LeaderboardFilters } from './leaderboardTypes'
import type { MetricId } from './metricTypes'

type MetricOptionsState = {
  filters: LeaderboardFilters
  seasons: string[]
  teams: TeamOption[]
}

type UseMetricOptionsValue = {
  filters: LeaderboardFilters
  availableSeasons: string[]
  availableTeams: TeamOption[]
  isBootstrapping: boolean
  loadBootstrapOptions: (metric: MetricId) => Promise<MetricOptionsState | null>
  refreshScopedOptions: (
    metric: MetricId,
    filters: LeaderboardFilters,
  ) => Promise<MetricOptionsState | null>
  setFilters: Dispatch<SetStateAction<LeaderboardFilters>>
}

export function useMetricOptions({
  metricRef,
  onStartBootstrap,
  onFinishBootstrap,
  onError,
  onSuccess,
}: {
  metricRef: MutableRefObject<MetricId>
  onStartBootstrap: () => void
  onFinishBootstrap: () => void
  onError: (message: string) => void
  onSuccess: () => void
}): UseMetricOptionsValue {
  const bootstrapAbortRef = useRef<AbortController | null>(null)
  const scopedOptionsAbortRef = useRef<AbortController | null>(null)

  const [filters, setFilters] = useState<LeaderboardFilters>(defaultLeaderboardFilters)
  const [availableTeams, setAvailableTeams] = useState<TeamOption[]>([])
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [isBootstrapping, setIsBootstrapping] = useState(true)

  useEffect(() => {
    return () => {
      bootstrapAbortRef.current?.abort()
      scopedOptionsAbortRef.current?.abort()
    }
  }, [])

  async function loadBootstrapOptions(metric: MetricId): Promise<MetricOptionsState | null> {
    return _runAbortableRequest({
      abortRef: bootstrapAbortRef,
      metricRef,
      expectedMetric: metric,
      onStart: () => {
        setIsBootstrapping(true)
        onStartBootstrap()
      },
      onFinish: () => {
        setIsBootstrapping(false)
        onFinishBootstrap()
      },
      request: (signal) => fetchMetricOptions(metric, undefined, signal),
      onSuccess: (payload) => {
        const nextFilters = initializeLeaderboardFiltersWithOptions(
          defaultLeaderboardFilters(),
          payload,
        )
        setFilters(nextFilters)
        setAvailableTeams(payload.team_options)
        setAvailableSeasons(payload.available_seasons)
        onSuccess()
        return {
          filters: nextFilters,
          seasons: payload.available_seasons,
          teams: payload.team_options,
        }
      },
      onError: (message) => {
        onError(message)
        return null
      },
    })
  }

  async function refreshScopedOptions(
    metric: MetricId,
    filters: LeaderboardFilters,
  ): Promise<MetricOptionsState | null> {
    return _runAbortableRequest({
      abortRef: scopedOptionsAbortRef,
      metricRef,
      expectedMetric: metric,
      onStart: () => {},
      onFinish: () => {},
      request: async (signal) => {
        const seasonsPayload = await fetchMetricOptions(
          metric,
          buildMetricOptionsParamsForTeams(filters.teamIds),
          signal,
        )
        const seasons = seasonsPayload.available_seasons
        const filtersAfterSeasonScope = syncScopedLeaderboardFiltersWithOptions(
          filters,
          seasonsPayload,
        )
        const teamsPayload = await fetchMetricOptions(
          metric,
          buildMetricOptionsParamsForSeasonSpan(filtersAfterSeasonScope, seasons),
          signal,
        )

        return {
          filters: syncScopedLeaderboardFiltersWithOptions(filtersAfterSeasonScope, teamsPayload),
          seasons,
          teams: teamsPayload.team_options,
        }
      },
      onSuccess: (payload) => {
        setFilters(payload.filters)
        setAvailableSeasons(payload.seasons)
        setAvailableTeams(payload.teams)
        onSuccess()
        return payload
      },
      onError: (message) => {
        onError(message)
        return null
      },
    })
  }

  return {
    filters,
    availableSeasons,
    availableTeams,
    isBootstrapping,
    loadBootstrapOptions,
    refreshScopedOptions,
    setFilters,
  }
}

function _isAborted(
  currentMetric: MetricId,
  expectedMetric: MetricId,
  controller: AbortController,
): boolean {
  return currentMetric !== expectedMetric || controller.signal.aborted
}

function _isExpectedAbort(caughtError: unknown): boolean {
  return caughtError instanceof DOMException && caughtError.name === 'AbortError'
}

type _AbortableRequestOptions<TRequest, TResult> = {
  abortRef: MutableRefObject<AbortController | null>
  metricRef: MutableRefObject<MetricId>
  expectedMetric: MetricId
  onStart: () => void
  onFinish: () => void
  request: (signal: AbortSignal) => Promise<TRequest>
  onSuccess: (payload: TRequest) => TResult
  onError: (message: string) => TResult | null
}

async function _runAbortableRequest<TRequest, TResult>({
  abortRef,
  metricRef,
  expectedMetric,
  onStart,
  onFinish,
  request,
  onSuccess,
  onError,
}: _AbortableRequestOptions<TRequest, TResult>): Promise<TResult | null> {
  abortRef.current?.abort()
  const controller = new AbortController()
  abortRef.current = controller
  onStart()

  try {
    const payload = await request(controller.signal)
    if (_isAborted(metricRef.current, expectedMetric, controller)) {
      return null
    }

    return onSuccess(payload)
  } catch (caughtError) {
    if (_isExpectedAbort(caughtError) || _isAborted(metricRef.current, expectedMetric, controller)) {
      return null
    }

    const message = caughtError instanceof Error ? caughtError.message : 'Request failed'
    return onError(message)
  } finally {
    if (abortRef.current === controller) {
      abortRef.current = null
      onFinish()
    }
  }
}
