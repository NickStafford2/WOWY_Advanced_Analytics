import { useEffect, useEffectEvent, useRef, useState, type MutableRefObject } from 'react'
import { fetchMetricOptions } from './api'
import {
  defaultLeaderboardFilters,
  initializeLeaderboardFiltersWithOptions,
  syncScopedLeaderboardFiltersWithOptions,
  updateLeaderboardFilterValue,
} from './leaderboardFilters'
import {
  buildMetricOptionsParamsForSeasonSpan,
  buildMetricOptionsParamsForTeams,
  buildExportUrl,
} from './leaderboardParams'
import { toggleLeaderboardSeasonType } from './leaderboardSeason'
import {
  isAllTeamsSelection,
  selectAllTeams,
  toggleSelectedTeam,
} from './leaderboardTeams'
import { metricDescriptionFor, metricLabelFor, metricStandsFor } from './metric'
import type { LeaderboardPayload, TeamOption } from './leaderboardApiTypes'
import type {
  LeaderboardFilters,
  LeaderboardNumberField,
  LeaderboardSeasonType,
} from './leaderboardTypes'
import type { MetricId } from './metricTypes'
import { useLoadingPanel } from './useLoadingPanel'
import { useLeaderboardRequest } from './useLeaderboardRequest'

type UseLeaderboardPageValue = {
  metric: MetricId
  metricDescription: string
  metricLabel: string
  metricStandsFor: string
  filters: LeaderboardFilters
  availableSeasons: string[]
  availableTeams: TeamOption[]
  leaderboard: LeaderboardPayload | null
  exportUrl: string
  error: string
  isBootstrapping: boolean
  isLoading: boolean
  isRawrMetric: boolean
  loadingPanel: ReturnType<typeof useLoadingPanel>['loadingPanel']
  setMetric: (metric: MetricId) => void
  setStartSeason: (season: string) => void
  setEndSeason: (season: string) => void
  toggleSeasonType: (seasonType: LeaderboardSeasonType) => void
  selectAllTeams: () => void
  toggleTeam: (teamId: number) => void
  setNumberFilter: (field: LeaderboardNumberField, value: number) => void
  refresh: () => Promise<void>
}

type MetricOptionsState = {
  filters: LeaderboardFilters
  seasons: string[]
  teams: TeamOption[]
}

export function useLeaderboardPage(): UseLeaderboardPageValue {
  const metricRef = useRef<MetricId>('wowy')
  const bootstrapAbortRef = useRef<AbortController | null>(null)
  const scopedOptionsAbortRef = useRef<AbortController | null>(null)

  const [metric, setMetricState] = useState<MetricId>('wowy')
  const [filters, setFilters] = useState<LeaderboardFilters>(defaultLeaderboardFilters)
  const [teamOptions, setTeamOptions] = useState<TeamOption[]>([])
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [isBootstrapping, setIsBootstrapping] = useState(true)
  const {
    leaderboard,
    error,
    isLoading,
    serverProgress,
    clearRequestState,
    setRequestError,
    runRequest,
  } = useLeaderboardRequest({ metricRef })

  const metricLabel = metricLabelFor(metric)
  const metricDescription = metricDescriptionFor(metric)
  const metricStandsForLabel = metricStandsFor(metric)
  const isRawrMetric = metric === 'rawr'
  const exportUrl = buildExportUrl({
    metric,
    filters,
    availableSeasons,
    availableTeams: teamOptions,
  })

  const { loadingPanel, restartLoadingClock } = useLoadingPanel({
    metric,
    metricLabel,
    isBootstrapping,
    isLoading,
    serverProgress,
  })

  useEffect(() => {
    return () => {
      bootstrapAbortRef.current?.abort()
      scopedOptionsAbortRef.current?.abort()
    }
  }, [])

  const _loadBootstrapOptions = useEffectEvent(
    async (nextMetric: MetricId): Promise<MetricOptionsState | null> =>
      _runAbortableRequest({
        abortRef: bootstrapAbortRef,
        metricRef,
        expectedMetric: nextMetric,
        onStart: () => {
          setIsBootstrapping(true)
          setRequestError('')
          clearRequestState()
          restartLoadingClock()
        },
        onFinish: () => {
          setIsBootstrapping(false)
        },
        request: (signal) => fetchMetricOptions(nextMetric, undefined, signal),
        onSuccess: (payload) => {
          const nextFilters = initializeLeaderboardFiltersWithOptions(
            defaultLeaderboardFilters(),
            payload,
          )
          setFilters(nextFilters)
          setTeamOptions(payload.team_options)
          setAvailableSeasons(payload.available_seasons)
          return {
            filters: nextFilters,
            seasons: payload.available_seasons,
            teams: payload.team_options,
          }
        },
        onError: (message) => {
          setRequestError(message)
          return null
        },
      }),
  )

  const _refreshScopedOptions = useEffectEvent(
    async (nextMetric: MetricId, nextFilters: LeaderboardFilters) =>
      _runAbortableRequest({
        abortRef: scopedOptionsAbortRef,
        metricRef,
        expectedMetric: nextMetric,
        onStart: () => { },
        onFinish: () => { },
        request: async (signal) => {
          const seasonsPayload = await fetchMetricOptions(
            nextMetric,
            buildMetricOptionsParamsForTeams(nextFilters.teamIds),
            signal,
          )
          const seasons = seasonsPayload.available_seasons
          const filtersAfterSeasonScope = syncScopedLeaderboardFiltersWithOptions(
            nextFilters,
            seasonsPayload,
          )
          const teamsPayload = await fetchMetricOptions(
            nextMetric,
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
          setRequestError('')
          setAvailableSeasons(payload.seasons)
          setTeamOptions(payload.teams)
          setFilters(payload.filters)
          return payload
        },
        onError: (message) => {
          setRequestError(message)
          return null
        },
      }),
  )

  const _loadMetric = useEffectEvent(async (nextMetric: MetricId) => {
    clearRequestState()

    const options = await _loadBootstrapOptions(nextMetric)
    if (options === null) {
      return
    }

    await runRequest({
      metric: nextMetric,
      filters: options.filters,
      availableSeasons: options.seasons,
      availableTeams: options.teams,
      restartLoadingClock,
    })
  })

  const _applyScopedFilterChange = useEffectEvent(
    async (buildNextFilters: (current: LeaderboardFilters) => LeaderboardFilters) => {
      const nextFilters = buildNextFilters(filters)
      setFilters(nextFilters)
      const scopedOptions = await _refreshScopedOptions(metric, nextFilters)
      if (scopedOptions === null) {
        return
      }
      await runRequest({
        metric,
        filters: scopedOptions.filters,
        availableSeasons: scopedOptions.seasons,
        availableTeams: scopedOptions.teams,
        restartLoadingClock,
      })
    },
  )

  useEffect(() => {
    void _loadMetric(metric)
  }, [metric])

  async function refresh(): Promise<void> {
    await runRequest({
      metric,
      filters,
      availableSeasons,
      availableTeams: teamOptions,
      restartLoadingClock,
    })
  }

  function setStartSeason(season: string): void {
    void _applyScopedFilterChange((current) => ({ ...current, startSeason: season }))
  }

  function setEndSeason(season: string): void {
    void _applyScopedFilterChange((current) => ({ ...current, endSeason: season }))
  }

  function toggleSeasonType(seasonType: LeaderboardSeasonType): void {
    const nextFilters = {
      ...filters,
      seasonTypes: toggleLeaderboardSeasonType(filters.seasonTypes, seasonType),
    }
    setFilters(nextFilters)
  }

  function handleSelectAllTeams(): void {
    void _applyScopedFilterChange((current) => ({
      ...current,
      teamIds: isAllTeamsSelection(current.teamIds, teamOptions) ? [] : selectAllTeams(),
    }))
  }

  function handleToggleTeam(teamId: number): void {
    void _applyScopedFilterChange((current) => ({
      ...current,
      teamIds: toggleSelectedTeam(current.teamIds, teamId, teamOptions),
    }))
  }

  function setNumberFilter(field: LeaderboardNumberField, value: number): void {
    setFilters((current) => updateLeaderboardFilterValue(current, field, value))
  }

  function handleMetricChange(nextMetric: MetricId): void {
    metricRef.current = nextMetric
    setMetricState(nextMetric)
  }

  return {
    metric,
    metricDescription,
    metricLabel,
    metricStandsFor: metricStandsForLabel,
    filters,
    availableSeasons,
    availableTeams: teamOptions,
    leaderboard,
    exportUrl,
    error,
    isBootstrapping,
    isLoading,
    isRawrMetric,
    loadingPanel,
    setMetric: handleMetricChange,
    setStartSeason,
    setEndSeason,
    toggleSeasonType,
    selectAllTeams: handleSelectAllTeams,
    toggleTeam: handleToggleTeam,
    setNumberFilter,
    refresh,
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
