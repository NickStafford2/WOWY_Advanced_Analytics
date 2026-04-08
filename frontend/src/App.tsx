import { useEffect, useEffectEvent, useMemo, useRef, useState } from 'react'
import type { Dispatch, SetStateAction } from 'react'
import { fetchLeaderboard, fetchMetricOptions } from './app/api'
import {
  buildAvailableTeamsForSeasonSpan,
  buildExportUrl,
  defaultLeaderboardFilters,
  filterSelectedTeamIdsForAvailableTeams,
  isAllTeamsSelection,
  metricDescriptionFor,
  metricLabelFor,
  selectAllTeams,
  syncLeaderboardFiltersWithOptions,
  syncSelectedTeamIds,
  toggleSelectedTeam,
  updateLeaderboardFilterValue,
} from './app/query'
import { useLoadingPanel } from './app/useLoadingPanel'
import type {
  LeaderboardFilters,
  LeaderboardNumberField,
  LeaderboardPayload,
  MetricId,
  TeamOption,
  ThemeMode,
} from './app/types'
import { About } from './components/About'
import { AppHeader } from './components/AppHeader'
import { LeaderboardFiltersPanel } from './components/LeaderboardFiltersPanel'
import { ResultsPanel } from './components/ResultsPanel'
import './App.css'

const THEME_STORAGE_KEY = 'wowy-theme'

function App() {
  const metricRef = useRef<MetricId>('wowy')
  const bootstrappingRequestRef = useRef(0)
  const leaderboardRequestRef = useRef(0)
  const [theme, setTheme] = useState<ThemeMode>(resolveInitialTheme)
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
    document.documentElement.dataset.theme = theme
    window.localStorage.setItem(THEME_STORAGE_KEY, theme)
  }, [theme])

  const loadOptions = useEffectEvent(
    async (
      nextMetric: MetricId,
    ): Promise<{ filters: LeaderboardFilters; seasons: string[]; teams: TeamOption[] } | null> => {
      const requestId = bootstrappingRequestRef.current + 1
      bootstrappingRequestRef.current = requestId
      setIsBootstrapping(true)
      setError('')
      restartLoadingClock()

      try {
        const payload = await fetchMetricOptions(nextMetric)
        if (
          metricRef.current !== nextMetric ||
          bootstrappingRequestRef.current !== requestId
        ) {
          return null
        }

        const nextFilters = syncLeaderboardFiltersWithOptions(filters, payload)
        setFilters(nextFilters)
        setTeamOptions(payload.team_options)
        setAvailableSeasons(payload.available_seasons)
        return {
          filters: nextFilters,
          seasons: payload.available_seasons,
          teams: payload.team_options,
        }
      } catch (caughtError) {
        if (
          metricRef.current !== nextMetric ||
          bootstrappingRequestRef.current !== requestId
        ) {
          return null
        }

        const message = caughtError instanceof Error ? caughtError.message : 'Request failed'
        setError(message)
        setLeaderboard(null)
        return null
      } finally {
        if (bootstrappingRequestRef.current === requestId) {
          setIsBootstrapping(false)
        }
      }
    },
  )

  async function runLeaderboardRequest(
    nextMetric: MetricId,
    nextFilters: LeaderboardFilters,
    seasons: string[],
    teams: TeamOption[],
  ): Promise<LeaderboardPayload | null> {
    const requestId = leaderboardRequestRef.current + 1
    leaderboardRequestRef.current = requestId
    const scopedTeams = buildAvailableTeamsForSeasonSpan({
      startSeason: nextFilters.startSeason,
      endSeason: nextFilters.endSeason,
      availableSeasons: seasons,
      teamOptions: teams,
    })
    const selectedTeamIds = filterSelectedTeamIdsForAvailableTeams(nextFilters.teamIds, scopedTeams)
    const hasSelectedTeams =
      scopedTeams.length > 0 &&
      (isAllTeamsSelection(nextFilters.teamIds, scopedTeams) || selectedTeamIds.length > 0)

    if (!hasSelectedTeams) {
      setError('Select at least one team active across the full season span.')
      setLeaderboard(null)
      return null
    }

    setError('')
    setIsLoading(true)
    restartLoadingClock()

    try {
      const payload = await fetchLeaderboard(nextMetric, nextFilters, seasons, scopedTeams)
      if (
        metricRef.current !== nextMetric ||
        leaderboardRequestRef.current !== requestId
      ) {
        return null
      }

      setLeaderboard(payload)
      return payload
    } catch (caughtError) {
      if (
        metricRef.current !== nextMetric ||
        leaderboardRequestRef.current !== requestId
      ) {
        return null
      }

      const message = caughtError instanceof Error ? caughtError.message : 'Request failed'
      setError(message)
      setLeaderboard(null)
      return null
    } finally {
      if (leaderboardRequestRef.current === requestId) {
        setIsLoading(false)
      }
    }
  }

  const loadMetric = useEffectEvent(async (nextMetric: MetricId) => {
    setLeaderboard(null)
    const options = await loadOptions(nextMetric)
    if (options === null) {
      return
    }

    await runLeaderboardRequest(
      nextMetric,
      options.filters,
      options.seasons,
      options.teams,
    )
  })

  async function refreshLeaderboard(): Promise<void> {
    await runLeaderboardRequest(metric, filters, availableSeasons, teamOptions)
  }

  useEffect(() => {
    void loadMetric(metric)
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

  return (
    <main className="app-shell">
      <div className="page-wrap">
        <AppHeader
          metric={metric}
          metricLabel={metricLabel}
          metricDescription={metricDescription}
          seasonCount={availableSeasons.length}
          teamCount={teamOptions.length}
          theme={theme}
          onMetricChange={setMetric}
          onThemeToggle={() =>
            setTheme((current) => (current === 'light' ? 'dark' : 'light'))
          }
        />

        <section className="dashboard-grid">
          <LeaderboardFiltersPanel
            filters={filters}
            availableSeasons={availableSeasons}
            availableTeams={availableTeams}
            isBootstrapping={isBootstrapping}
            isLoading={isLoading}
            isRawrMetric={isRawrMetric}
            onStartSeasonChange={(season) =>
              setFilters((current) => ({ ...current, startSeason: season }))
            }
            onEndSeasonChange={(season) =>
              setFilters((current) => ({ ...current, endSeason: season }))
            }
            onSelectAllTeams={() =>
              setFilters((current) => ({
                ...current,
                teamIds: selectAllTeams(),
              }))
            }
            onToggleTeam={(teamId) =>
              setFilters((current) => ({
                ...current,
                teamIds: toggleSelectedTeam(current.teamIds, teamId, availableTeams),
              }))
            }
            onNumberChange={(field, value) =>
              handleLeaderboardNumberChange(setFilters, field, value)
            }
            onRefresh={() => void refreshLeaderboard()}
          />

          <ResultsPanel
            metric={metric}
            metricLabel={metricLabel}
            leaderboard={leaderboard}
            exportUrl={exportUrl}
            error={error}
            isLoading={isLoading}
            isBootstrapping={isBootstrapping}
            loadingPanel={loadingPanel}
          />
        </section>

        <About />

        <footer className="page-footer">
          <span className="footer-name">Nicholas Stafford</span>
          <span className="footer-note">
            Simple frontend shell for cache-backed leaderboard queries.
          </span>
        </footer>
      </div>
    </main>
  )
}

function handleLeaderboardNumberChange(
  setFilters: Dispatch<SetStateAction<LeaderboardFilters>>,
  field: LeaderboardNumberField,
  value: number,
): void {
  setFilters((current) => updateLeaderboardFilterValue(current, field, value))
}

function resolveInitialTheme(): ThemeMode {
  const storedTheme = window.localStorage.getItem(THEME_STORAGE_KEY)
  if (storedTheme === 'light' || storedTheme === 'dark') {
    return storedTheme
  }
  return 'light'
}

export default App
