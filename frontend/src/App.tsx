import { useLeaderboardPage } from './app/useLeaderboardPage'
import { useTheme } from './app/useTheme'
import { About } from './components/About'
import { AppHeader } from './components/AppHeader'
import { LeaderboardFiltersPanel } from './components/LeaderboardFiltersPanel'
import { ResultsPanel } from './components/ResultsPanel'

function App() {
  const {
    metric,
    metricDescription,
    metricLabel,
    metricStandsFor,
    filters,
    availableSeasons,
    availableTeams,
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
    selectAllTeams,
    toggleTeam,
    setNumberFilter,
    refresh,
  } = useLeaderboardPage()
  const { theme, toggleTheme } = useTheme()

  return (
    <main className="w-full">
      <div className="mx-auto w-[min(1280px,calc(100vw-32px))] px-0 pt-6 pb-14 max-sm:w-[min(1280px,calc(100vw-20px))] max-sm:pt-4">
        <AppHeader
          metric={metric}
          metricLabel={metricLabel}
          metricStandsFor={metricStandsFor}
          metricDescription={metricDescription}
          onMetricChange={setMetric}
        />

        <section className="mt-5 grid grid-cols-1 items-start gap-5">
          <LeaderboardFiltersPanel
            filters={filters}
            availableSeasons={availableSeasons}
            availableTeams={availableTeams}
            isBootstrapping={isBootstrapping}
            isLoading={isLoading}
            isRawrMetric={isRawrMetric}
            onStartSeasonChange={setStartSeason}
            onEndSeasonChange={setEndSeason}
            onSelectAllTeams={selectAllTeams}
            onToggleTeam={toggleTeam}
            onNumberChange={setNumberFilter}
            onRefresh={() => void refresh()}
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

        <About metric={metric} />

        <footer className="mt-5 flex items-center justify-between gap-4 rounded-[28px] border border-[color:var(--panel-border)] bg-[var(--panel-background)] px-5 py-[18px] shadow-[var(--panel-shadow)] max-sm:grid max-sm:grid-cols-1 max-sm:rounded-[22px] max-sm:px-[18px] max-sm:py-[18px]">
          <div className="flex flex-col gap-1.5">
            <span className="text-[0.92rem] font-bold tracking-[0.08em] uppercase">
              Nicholas Stafford
            </span>
            <span className="text-[color:var(--text-muted)]">
              Simple frontend shell for cache-backed leaderboard queries.
            </span>
          </div>
          <button
            type="button"
            className="min-h-[42px] cursor-pointer self-start rounded-[14px] border border-[color:var(--control-border)] bg-[var(--input-background)] px-[14px] font-bold text-[color:var(--text-secondary)] transition-colors disabled:cursor-not-allowed disabled:opacity-60 max-sm:self-auto"
            onClick={toggleTheme}
          >
            {theme === 'dark' ? 'Light mode' : 'Dark mode'}
          </button>
        </footer>
      </div>
    </main>
  )
}

export default App
