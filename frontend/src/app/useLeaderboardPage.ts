import { useEffect, useEffectEvent, useRef, useState, type MutableRefObject } from 'react'
import { fetchLeaderboard, fetchMetricOptions } from './api'
import {
  defaultLeaderboardFilters,
  initializeLeaderboardFiltersWithOptions,
  syncScopedLeaderboardFiltersWithOptions,
  updateLeaderboardFilterValue,
} from './leaderboardFilters'
import {
  buildLeaderboardStreamUrl,
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
import type { LeaderboardProgressEvent } from './loadingTypes'
import { metricDescriptionFor, metricLabelFor, metricStandsFor } from './metric'
import type { LeaderboardPayload, TeamOption } from './leaderboardApiTypes'
import type {
  LeaderboardFilters,
  LeaderboardNumberField,
  LeaderboardSeasonType,
} from './leaderboardTypes'
import type { MetricId } from './metricTypes'
import { streamRawrLeaderboard } from './rawrStream'
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

type StreamHandle = {
  close: () => void
}

export function useLeaderboardPage(): UseLeaderboardPageValue {
  const metricRef = useRef<MetricId>('wowy')
  const bootstrapAbortRef = useRef<AbortController | null>(null)
  const scopedOptionsAbortRef = useRef<AbortController | null>(null)
  const leaderboardAbortRef = useRef<AbortController | null>(null)
  const rawrStreamRef = useRef<StreamHandle | null>(null)

  const [metric, setMetricState] = useState<MetricId>('wowy')
  const [filters, setFilters] = useState<LeaderboardFilters>(defaultLeaderboardFilters)
  const [teamOptions, setTeamOptions] = useState<TeamOption[]>([])
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [leaderboard, setLeaderboard] = useState<LeaderboardPayload | null>(null)
  const [isBootstrapping, setIsBootstrapping] = useState(true)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')
  const [serverProgress, setServerProgress] = useState<LeaderboardProgressEvent | null>(null)

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
      leaderboardAbortRef.current?.abort()
      rawrStreamRef.current?.close()
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
          setError('')
          setServerProgress(null)
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
          setError(message)
          setLeaderboard(null)
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
    rawrStreamRef.current?.close()
    rawrStreamRef.current = null

    const selectedTeamIds = filterSelectedTeamIdsForAvailableTeams(nextFilters.teamIds, teams)
    const hasSelectedTeams = _hasSelectedTeams(nextFilters.teamIds, selectedTeamIds, teams)

    console.log('[Leaderboard] request start', {
      nextMetric,
      seasonCount: seasons.length,
      teamCount: teams.length,
      selectedTeamIds,
    })

    if (!hasSelectedTeams) {
      console.warn('[Leaderboard] blocked: no selected teams')
      setError('Select at least one team active across the full season span.')
      setLeaderboard(null)
      setServerProgress(null)
      return null
    }

    setError('')
    setIsLoading(true)
    setLeaderboard(null)
    setServerProgress(null)
    restartLoadingClock()

    if (nextMetric === 'rawr') {
      const streamUrl = buildLeaderboardStreamUrl({
        metric: nextMetric,
        filters: nextFilters,
        availableSeasons: seasons,
        availableTeams: teams,
      })

      console.log('[Leaderboard] USING SSE STREAM', { streamUrl })

      return await new Promise<LeaderboardPayload | null>((resolve) => {
        let finished = false

        const finish = (value: LeaderboardPayload | null) => {
          if (finished) {
            return
          }
          finished = true
          rawrStreamRef.current = null
          setIsLoading(false)
          console.log('[Leaderboard] stream finished', { hasPayload: value != null })
          resolve(value)
        }

        rawrStreamRef.current = streamRawrLeaderboard<LeaderboardPayload>({
          url: streamUrl,
          onStarted: (payload) => {
            console.log('[Leaderboard] stream started', payload)
          },
          onProgress: (progress) => {
            console.log('[Leaderboard] stream progress', progress)
            if (metricRef.current !== nextMetric) {
              return
            }
            setServerProgress(progress)
          },
          onResult: (payload) => {
            console.log('[Leaderboard] stream result', payload)
            if (metricRef.current !== nextMetric) {
              finish(null)
              return
            }
            setLeaderboard(payload)
            finish(payload)
          },
          onError: (message) => {
            console.error('[Leaderboard] stream error', message)
            if (metricRef.current !== nextMetric) {
              finish(null)
              return
            }
            setError(message)
            setLeaderboard(null)
            finish(null)
          },
        })
      })
    }

    console.log('[Leaderboard] USING JSON FETCH', { metric: nextMetric })

    return _runAbortableRequest({
      abortRef: leaderboardAbortRef,
      metricRef,
      expectedMetric: nextMetric,
      onStart: () => { },
      onFinish: () => {
        setIsLoading(false)
      },
      request: (signal) => fetchLeaderboard(nextMetric, nextFilters, seasons, teams, signal),
      onSuccess: (payload) => {
        console.log('[Leaderboard] fetch success')
        setLeaderboard(payload)
        return payload
      },
      onError: (message) => {
        console.error('[Leaderboard] fetch error', message)
        setError(message)
        setLeaderboard(null)
        return null
      },
    })
  }

  const _loadMetric = useEffectEvent(async (nextMetric: MetricId) => {
    setLeaderboard(null)
    setServerProgress(null)
    leaderboardAbortRef.current?.abort()
    rawrStreamRef.current?.close()
    rawrStreamRef.current = null

    const options = await _loadBootstrapOptions(nextMetric)
    if (options === null) {
      return
    }

    await _runLeaderboardRequest(nextMetric, options.filters, options.seasons, options.teams)
  })

  const _applyScopedFilterChange = useEffectEvent(
    async (buildNextFilters: (current: LeaderboardFilters) => LeaderboardFilters) => {
      const nextFilters = buildNextFilters(filters)
      setFilters(nextFilters)
      const scopedOptions = await _refreshScopedOptions(metric, nextFilters)
      if (scopedOptions === null) {
        return
      }
      await _runLeaderboardRequest(
        metric,
        scopedOptions.filters,
        scopedOptions.seasons,
        scopedOptions.teams,
      )
    },
  )

  useEffect(() => {
    void _loadMetric(metric)
  }, [metric])

  async function refresh(): Promise<void> {
    await _runLeaderboardRequest(metric, filters, availableSeasons, teamOptions)
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
