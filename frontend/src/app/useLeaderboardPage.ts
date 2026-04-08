import { useEffect, useEffectEvent, useMemo, useRef, useState } from 'react'
import { fetchLeaderboard, fetchMetricOptions } from './api'
import {
  buildAvailableTeamsForSeasonSpan,
  buildExportUrl,
  defaultLeaderboardFilters,
  filterSelectedTeamIdsForAvailableTeams,
  isAllTeamsSelection,
  selectAllTeams,
  syncLeaderboardFiltersWithOptions,
  syncSelectedTeamIds,
  toggleSelectedTeam,
  updateLeaderboardFilterValue,
} from './leaderboardQuery'
import { metricDescriptionFor, metricLabelFor } from './metric'
import type {
  LeaderboardFilters,
  LeaderboardNumberField,
  LeaderboardPayload,
  MetricId,
  TeamOption,
} from './types'
import { useLoadingPanel } from './useLoadingPanel'

type UseLeaderboardPageValue = {
  metric: MetricId
  metricDescription: string
  metricLabel: string
  filters: LeaderboardFilters
  availableSeasons: string[]
  availableTeams: TeamOption[]
  teamCount: number
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
  const optionsAbortRef = useRef<AbortController | null>(null)
  const leaderboardAbortRef = useRef<AbortController | null>(null)
  const [metric, setMetric] = useState<MetricId>('wowy')
  const [filters, setFilters] = useState<LeaderboardFilters>(defaultLeaderboardFilters)
  const [teamOptions, setTeamOptions] = useState<TeamOption[]>([])
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [leaderboard, setLeaderboard] = useState<LeaderboardPayload | null>(null)
  const [isBootstrapping, setIsBootstrapping] = useState(true)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')

  const metricLabel = metricLabelFor(metric)
  const metricDescription = metricDescriptionFor(metric)
  const availableTeams = useMemo(
    () =>
      buildAvailableTeamsForSeasonSpan({
        startSeason: filters.startSeason,
        endSeason: filters.endSeason,
        availableSeasons,
        teamOptions,
      }),
    [availableSeasons, filters.endSeason, filters.startSeason, teamOptions],
  )
  const isRawrMetric = metric === 'rawr'
  const exportUrl = buildExportUrl({
    metric,
    filters,
    availableSeasons,
    availableTeams,
  })
  const { loadingPanel, restartLoadingClock } = useLoadingPanel({
    metric,
    metricLabel,
    isBootstrapping,
    isLoading,
  })

  useEffect(() => {
    metricRef.current = metric
  }, [metric])

  useEffect(() => {
    return () => {
      optionsAbortRef.current?.abort()
      leaderboardAbortRef.current?.abort()
    }
  }, [])

  const _loadOptions = useEffectEvent(async (nextMetric: MetricId): Promise<MetricOptionsState | null> => {
    optionsAbortRef.current?.abort()
    const controller = new AbortController()
    optionsAbortRef.current = controller
    setIsBootstrapping(true)
    setError('')
    restartLoadingClock()

    try {
      const payload = await fetchMetricOptions(nextMetric, controller.signal)
      const nextFilters = syncLeaderboardFiltersWithOptions(filters, payload)
      if (_isAborted(metricRef.current, nextMetric, controller)) {
        return null
      }

      setFilters(nextFilters)
      setTeamOptions(payload.team_options)
      setAvailableSeasons(payload.available_seasons)
      return {
        filters: nextFilters,
        seasons: payload.available_seasons,
        teams: payload.team_options,
      }
    } catch (caughtError) {
      if (_isExpectedAbort(caughtError) || _isAborted(metricRef.current, nextMetric, controller)) {
        return null
      }

      const message = caughtError instanceof Error ? caughtError.message : 'Request failed'
      setError(message)
      setLeaderboard(null)
      return null
    } finally {
      if (optionsAbortRef.current === controller) {
        optionsAbortRef.current = null
        setIsBootstrapping(false)
      }
    }
  })

  async function _runLeaderboardRequest(
    nextMetric: MetricId,
    nextFilters: LeaderboardFilters,
    seasons: string[],
    teams: TeamOption[],
  ): Promise<LeaderboardPayload | null> {
    leaderboardAbortRef.current?.abort()
    const controller = new AbortController()
    leaderboardAbortRef.current = controller
    const scopedTeams = buildAvailableTeamsForSeasonSpan({
      startSeason: nextFilters.startSeason,
      endSeason: nextFilters.endSeason,
      availableSeasons: seasons,
      teamOptions: teams,
    })
    const selectedTeamIds = filterSelectedTeamIdsForAvailableTeams(nextFilters.teamIds, scopedTeams)
    const hasSelectedTeams = _hasSelectedTeams(nextFilters.teamIds, selectedTeamIds, scopedTeams)

    if (!hasSelectedTeams) {
      setError('Select at least one team active across the full season span.')
      setLeaderboard(null)
      return null
    }

    setError('')
    setIsLoading(true)
    restartLoadingClock()

    try {
      const payload = await fetchLeaderboard(
        nextMetric,
        nextFilters,
        seasons,
        scopedTeams,
        controller.signal,
      )
      if (_isAborted(metricRef.current, nextMetric, controller)) {
        return null
      }

      setLeaderboard(payload)
      return payload
    } catch (caughtError) {
      if (_isExpectedAbort(caughtError) || _isAborted(metricRef.current, nextMetric, controller)) {
        return null
      }

      const message = caughtError instanceof Error ? caughtError.message : 'Request failed'
      setError(message)
      setLeaderboard(null)
      return null
    } finally {
      if (leaderboardAbortRef.current === controller) {
        leaderboardAbortRef.current = null
        setIsLoading(false)
      }
    }
  }

  const _loadMetric = useEffectEvent(async (nextMetric: MetricId) => {
    setLeaderboard(null)
    leaderboardAbortRef.current?.abort()
    const options = await _loadOptions(nextMetric)
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
    setFilters((current) => {
      const nextTeamIds = syncSelectedTeamIds(current.teamIds, availableTeams)
      if (nextTeamIds.length === current.teamIds.length) {
        return current
      }

      return { ...current, teamIds: nextTeamIds }
    })
  }, [availableTeams])

  async function refresh(): Promise<void> {
    await _runLeaderboardRequest(metric, filters, availableSeasons, teamOptions)
  }

  function setStartSeason(season: string): void {
    setFilters((current) => ({ ...current, startSeason: season }))
  }

  function setEndSeason(season: string): void {
    setFilters((current) => ({ ...current, endSeason: season }))
  }

  function handleSelectAllTeams(): void {
    setFilters((current) => ({
      ...current,
      teamIds: selectAllTeams(),
    }))
  }

  function handleToggleTeam(teamId: number): void {
    setFilters((current) => ({
      ...current,
      teamIds: toggleSelectedTeam(current.teamIds, teamId, availableTeams),
    }))
  }

  function setNumberFilter(field: LeaderboardNumberField, value: number): void {
    setFilters((current) => updateLeaderboardFilterValue(current, field, value))
  }

  return {
    metric,
    metricDescription,
    metricLabel,
    filters,
    availableSeasons,
    availableTeams,
    teamCount: teamOptions.length,
    leaderboard,
    exportUrl,
    error,
    isBootstrapping,
    isLoading,
    isRawrMetric,
    loadingPanel,
    setMetric,
    setStartSeason,
    setEndSeason,
    selectAllTeams: handleSelectAllTeams,
    toggleTeam: handleToggleTeam,
    setNumberFilter,
    refresh,
  }
}

function _hasSelectedTeams(
  selectedTeamIds: number[],
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
