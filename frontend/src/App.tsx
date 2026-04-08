import { useEffect, useEffectEvent, useMemo, useRef, useState } from 'react'
import type { Dispatch, SetStateAction } from 'react'
import { fetchCachedLeaderboard, fetchCustomLeaderboard, fetchMetricOptions } from './app/api'
import {
  buildAvailableTeamsForSeasonSpan,
  buildExportUrl,
  defaultCustomFilters,
  defaultMetricFilters,
  filterSelectedTeamIdsForAvailableTeams,
  metricDescriptionFor,
  metricLabelFor,
  syncCustomFiltersWithOptions,
  syncSelectedTeamIds,
  toggleAllSelectedTeams,
  toggleSelectedTeam,
  updateMetricFilterValue,
} from './app/query'
import { useLoadingPanel } from './app/useLoadingPanel'
import type {
  AppMode,
  CachedFilters,
  CustomFilters,
  CustomNumberField,
  LeaderboardPayload,
  MetricFilters,
  MetricId,
  MetricNumberField,
  TeamOption,
  ThemeMode,
} from './app/types'
import { About } from './components/About'
import { AppHeader } from './components/AppHeader'
import { CachedFiltersPanel } from './components/CachedFiltersPanel'
import { CustomQueryPanel } from './components/CustomQueryPanel'
import { ResultsPanel } from './components/ResultsPanel'
import './App.css'

const THEME_STORAGE_KEY = 'wowy-theme'

function App() {
  const metricRef = useRef<MetricId>('wowy')
  const bootstrappingRequestRef = useRef(0)
  const leaderboardRequestRef = useRef(0)
  const [theme, setTheme] = useState<ThemeMode>(resolveInitialTheme)
  const [metric, setMetric] = useState<MetricId>('wowy')
  const [mode, setMode] = useState<AppMode>('cached')
  const [metricFilters, setMetricFilters] = useState<MetricFilters>(defaultMetricFilters('wowy'))
  const [teamOptions, setTeamOptions] = useState<TeamOption[]>([])
  const [availableSeasons, setAvailableSeasons] = useState<string[]>([])
  const [leaderboard, setLeaderboard] = useState<LeaderboardPayload | null>(null)
  const [cachedFilters, setCachedFilters] = useState<CachedFilters>({ teamId: null, topN: 12 })
  const [customFilters, setCustomFilters] = useState<CustomFilters>(defaultCustomFilters)
  const [isBootstrapping, setIsBootstrapping] = useState(true)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')

  const metricLabel = metricLabelFor(metric)
  const metricDescription = metricDescriptionFor(metric)

  const availableCustomTeams = useMemo(
    () =>
      buildAvailableTeamsForSeasonSpan({
        startSeason: customFilters.startSeason,
        endSeason: customFilters.endSeason,
        availableSeasons,
        teamOptions,
      }),
    [availableSeasons, customFilters.endSeason, customFilters.startSeason, teamOptions],
  )

  const allTeamsSelected =
    availableCustomTeams.length > 0 && customFilters.teams.length === availableCustomTeams.length
  const isRawrMetric = metric === 'rawr'
  const exportUrl = buildExportUrl({
    metric,
    mode,
    cachedFilters,
    customFilters,
    availableSeasons,
    availableCustomTeams,
    metricFilters,
  })
  const { loadingPanel, restartLoadingClock } = useLoadingPanel({
    metric,
    metricLabel,
    mode,
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

  const loadOptions = useEffectEvent(async (nextMetric: MetricId) => {
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

      setMetricFilters(payload.filters)
      setTeamOptions(payload.team_options)
      setAvailableSeasons(payload.available_seasons)
      setCachedFilters((current) => ({
        teamId: payload.team_options.some((team) => team.team_id === current.teamId)
          ? current.teamId
          : null,
        topN: current.topN || payload.filters.top_n,
      }))
      setCustomFilters((current) => syncCustomFiltersWithOptions(current, payload))
      return payload
    } finally {
      if (bootstrappingRequestRef.current === requestId) {
        setIsBootstrapping(false)
      }
    }
  })

  async function runLeaderboardRequest(
    nextMetric: MetricId,
    request: () => Promise<LeaderboardPayload>,
  ): Promise<LeaderboardPayload | null> {
    const requestId = leaderboardRequestRef.current + 1
    leaderboardRequestRef.current = requestId
    setError('')
    setIsLoading(true)
    restartLoadingClock()

    try {
      const payload = await request()
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

    await runLeaderboardRequest(nextMetric, () =>
      fetchCachedLeaderboard(nextMetric, cachedFilters, options.filters),
    )
  })

  async function refreshCachedLeaderboard(): Promise<void> {
    await runLeaderboardRequest(metric, () =>
      fetchCachedLeaderboard(metric, cachedFilters, metricFilters),
    )
  }

  async function runCustomQuery(): Promise<void> {
    const selectedTeamIds = filterSelectedTeamIdsForAvailableTeams(
      customFilters.teams,
      availableCustomTeams,
    )
    if (selectedTeamIds.length === 0) {
      setError('Select at least one team active across the full season span.')
      setLeaderboard(null)
      return
    }

    await runLeaderboardRequest(metric, () =>
      fetchCustomLeaderboard(metric, customFilters, availableSeasons, availableCustomTeams),
    )
  }

  useEffect(() => {
    void loadMetric(metric)
  }, [metric])

  useEffect(() => {
    setCustomFilters((current) => {
      const nextTeams = syncSelectedTeamIds(current.teams, availableCustomTeams)
      if (nextTeams.length === current.teams.length) {
        return current
      }

      return { ...current, teams: nextTeams }
    })
  }, [availableCustomTeams])

  return (
    <main className="app-shell">
      <div className="page-wrap">
        <AppHeader
          metric={metric}
          mode={mode}
          metricLabel={metricLabel}
          metricDescription={metricDescription}
          seasonCount={availableSeasons.length}
          teamCount={teamOptions.length}
          theme={theme}
          onMetricChange={setMetric}
          onModeChange={setMode}
          onThemeToggle={() =>
            setTheme((current) => (current === 'light' ? 'dark' : 'light'))
          }
        />

        <section className="dashboard-grid">
          {mode === 'cached' ? (
            <CachedFiltersPanel
              cachedFilters={cachedFilters}
              metricFilters={metricFilters}
              availableTeams={teamOptions}
              isBootstrapping={isBootstrapping}
              isLoading={isLoading}
              isRawrMetric={isRawrMetric}
              onTeamChange={(teamId) =>
                setCachedFilters((current) => ({ ...current, teamId }))
              }
              onTopNChange={(value) =>
                setCachedFilters((current) => ({ ...current, topN: value }))
              }
              onMetricFilterChange={(field, value) =>
                handleMetricFilterChange(setMetricFilters, field, value)
              }
              onRefresh={() => void refreshCachedLeaderboard()}
            />
          ) : (
            <CustomQueryPanel
              customFilters={customFilters}
              availableSeasons={availableSeasons}
              availableTeams={availableCustomTeams}
              isBootstrapping={isBootstrapping}
              isLoading={isLoading}
              isRawrMetric={isRawrMetric}
              allTeamsSelected={allTeamsSelected}
              onStartSeasonChange={(season) =>
                setCustomFilters((current) => ({ ...current, startSeason: season }))
              }
              onEndSeasonChange={(season) =>
                setCustomFilters((current) => ({ ...current, endSeason: season }))
              }
              onToggleAllTeams={() =>
                setCustomFilters((current) => ({
                  ...current,
                  teams: toggleAllSelectedTeams(current.teams, availableCustomTeams),
                }))
              }
              onToggleTeam={(teamId) =>
                setCustomFilters((current) => ({
                  ...current,
                  teams: toggleSelectedTeam(current.teams, teamId),
                }))
              }
              onNumberChange={(field, value) =>
                handleCustomNumberChange(setCustomFilters, field, value)
              }
              onRunQuery={() => void runCustomQuery()}
            />
          )}

          <ResultsPanel
            metric={metric}
            metricLabel={metricLabel}
            mode={mode}
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
            Simple frontend shell for cached boards and live cross-season queries.
          </span>
        </footer>
      </div>
    </main>
  )
}

function handleCustomNumberChange(
  setCustomFilters: Dispatch<SetStateAction<CustomFilters>>,
  field: CustomNumberField,
  value: number,
): void {
  setCustomFilters((current) => ({
    ...current,
    [field]: Number.isFinite(value) ? value : 0,
  }))
}

function handleMetricFilterChange(
  setMetricFilters: Dispatch<SetStateAction<MetricFilters>>,
  field: MetricNumberField,
  value: number,
): void {
  setMetricFilters((current) => updateMetricFilterValue(current, field, value))
}

function resolveInitialTheme(): ThemeMode {
  const storedTheme = window.localStorage.getItem(THEME_STORAGE_KEY)
  if (storedTheme === 'light' || storedTheme === 'dark') {
    return storedTheme
  }
  return 'light'
}

export default App
