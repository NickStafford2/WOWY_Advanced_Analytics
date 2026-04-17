import { useEffect, useEffectEvent, useRef, useState } from 'react'
import { updateLeaderboardFilterValue } from './leaderboardFilters'
import { buildExportUrl } from './leaderboardParams'
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
import { useMetricOptions } from './useMetricOptions'

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

export function useLeaderboardPage(): UseLeaderboardPageValue {
  const metricRef = useRef<MetricId>('wowy')

  const [metric, setMetricState] = useState<MetricId>('wowy')
  const {
    leaderboard,
    error,
    isLoading,
    serverProgress,
    clearRequestState,
    setRequestError,
    runRequest,
  } = useLeaderboardRequest({ metricRef })
  const {
    filters,
    availableSeasons,
    availableTeams: teamOptions,
    isBootstrapping,
    loadBootstrapOptions,
    refreshScopedOptions,
    setFilters,
  } = useMetricOptions({
    metricRef,
    onStartBootstrap: () => {
      setRequestError('')
      clearRequestState()
      restartLoadingClock()
    },
    onFinishBootstrap: () => {},
    onError: setRequestError,
    onSuccess: () => {
      setRequestError('')
    },
  })

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

  const _loadMetric = useEffectEvent(async (nextMetric: MetricId) => {
    clearRequestState()

    const options = await loadBootstrapOptions(nextMetric)
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
      const scopedOptions = await refreshScopedOptions(metric, nextFilters)
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
