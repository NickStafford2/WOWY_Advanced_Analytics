import { useEffect, useEffectEvent, useRef, useState, type MutableRefObject } from 'react'
import { fetchLeaderboard, fetchMetricOptions } from './api'
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
  filterSelectedTeamIdsForAvailableTeams,
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
  const leaderboardAbortRef = useRef<AbortController | null>(null)
  const [metric, setMetricState] = useState<MetricId>('wowy')
  const [filters, setFilters] = useState<LeaderboardFilters>(defaultLeaderboardFilters)
  const [teamOptions, setTeamOptions] = useState<TeamOption[]>([])
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [leaderboard, setLeaderboard] = useState<LeaderboardPayload | null>(null)
  const [isBootstrapping, setIsBootstrapping] = useState(true)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')

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
  })

  useEffect(() => {
    return () => {
      bootstrapAbortRef.current?.abort()
      scopedOptionsAbortRef.current?.abort()
      leaderboardAbortRef.current?.abort()
    }
  }, [])

  const _loadBootstrapOptions = useEffectEvent(async (nextMetric: MetricId): Promise<MetricOptionsState | null> =>
    _runAbortableRequest({
      abortRef: bootstrapAbortRef,
      metricRef,
      expectedMetric: nextMetric,
      onStart: () => {
        setIsBootstrapping(true)
        setError('')
        restartLoadingClock()
      },
      onFinish: () => {
        setIsBootstrapping(false)
      },
      request: (signal) => fetchMetricOptions(nextMetric, undefined, signal),
      onSuccess: (payload) => {
        const nextFilters = initializeLeaderboardFiltersWithOptions(defaultLeaderboardFilters(), payload)
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
        setError(message)
        setLeaderboard(null)
        return null
      },
    }),
  )

  const _refreshScopedOptions = useEffectEvent(async (nextMetric: MetricId, nextFilters: LeaderboardFilters) =>
    _runAbortableRequest({
      abortRef: scopedOptionsAbortRef,
      metricRef,
      expectedMetric: nextMetric,
      onStart: () => {},
      onFinish: () => {},
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
        setError('')
        setAvailableSeasons(payload.seasons)
        setTeamOptions(payload.teams)
        setFilters(payload.filters)
        return payload
      },
      onError: (message) => {
        setError(message)
        return null
      },
    }),
  )

  async function _runLeaderboardRequest(
    nextMetric: MetricId,
    nextFilters: LeaderboardFilters,
    seasons: string[],
    teams: TeamOption[],
  ): Promise<LeaderboardPayload | null> {
    leaderboardAbortRef.current?.abort()
    const selectedTeamIds = filterSelectedTeamIdsForAvailableTeams(nextFilters.teamIds, teams)
    const hasSelectedTeams = _hasSelectedTeams(nextFilters.teamIds, selectedTeamIds, teams)

    if (!hasSelectedTeams) {
      setError('Select at least one team active across the full season span.')
      setLeaderboard(null)
      return null
    }

    return _runAbortableRequest({
      abortRef: leaderboardAbortRef,
      metricRef,
      expectedMetric: nextMetric,
      onStart: () => {
        setError('')
        setIsLoading(true)
        restartLoadingClock()
      },
      onFinish: () => {
        setIsLoading(false)
      },
      request: (signal) =>
        fetchLeaderboard(nextMetric, nextFilters, seasons, teams, signal),
      onSuccess: (payload) => {
        setLeaderboard(payload)
        return payload
      },
      onError: (message) => {
        setError(message)
        setLeaderboard(null)
        return null
      },
    })
  }

  const _loadMetric = useEffectEvent(async (nextMetric: MetricId) => {
    setLeaderboard(null)
    leaderboardAbortRef.current?.abort()
    const options = await _loadBootstrapOptions(nextMetric)
    if (options === null) {
      return
    }

    await _runLeaderboardRequest(
      nextMetric,
      options.filters,
      options.seasons,
      options.teams,
    )
  })

  useEffect(() => {
    void _loadMetric(metric)
  }, [metric])

  useEffect(() => {
    if (isBootstrapping) {
      return
    }
    void _refreshScopedOptions(metric, filters)
  }, [
    filters.endSeason,
    filters.startSeason,
    filters.teamIds,
    isBootstrapping,
    metric,
    _refreshScopedOptions,
  ])

  async function refresh(): Promise<void> {
    await _runLeaderboardRequest(metric, filters, availableSeasons, teamOptions)
  }

  function setStartSeason(season: string): void {
    setFilters((current) => ({ ...current, startSeason: season }))
  }

  function setEndSeason(season: string): void {
    setFilters((current) => ({ ...current, endSeason: season }))
  }

  function toggleSeasonType(seasonType: LeaderboardSeasonType): void {
    setFilters((current) => ({
      ...current,
      seasonTypes: toggleLeaderboardSeasonType(current.seasonTypes, seasonType),
    }))
  }

  function handleSelectAllTeams(): void {
    setFilters((current) => ({
      ...current,
      teamIds: isAllTeamsSelection(current.teamIds, teamOptions) ? [] : selectAllTeams(),
    }))
  }

  function handleToggleTeam(teamId: number): void {
    setFilters((current) => ({
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

function _hasSelectedTeams(
  selectedTeamIds: number[] | null,
  scopedTeamIds: number[],
  availableTeams: TeamOption[],
): boolean {
  return (
    availableTeams.length > 0 &&
    (isAllTeamsSelection(selectedTeamIds, availableTeams) || scopedTeamIds.length > 0)
  )
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
