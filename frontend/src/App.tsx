import { useLeaderboardPage } from './app/useLeaderboardPage'
import { useTheme } from './app/useTheme'
import { About } from './components/About'
import { AppHeader } from './components/AppHeader'
import { LeaderboardFiltersPanel } from './components/LeaderboardFiltersPanel'
import { ResultsPanel } from './components/ResultsPanel'
import './App.css'

function App() {
  const {
    metric,
    metricDescription,
    metricLabel,
    filters,
    availableSeasons,
    availableTeams,
    teamCount,
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
    <main className="app-shell">
      <div className="page-wrap">
        <AppHeader
          metric={metric}
          metricLabel={metricLabel}
          metricDescription={metricDescription}
          seasonCount={availableSeasons.length}
          teamCount={teamCount}
          theme={theme}
          onMetricChange={setMetric}
          onThemeToggle={toggleTheme}
        />

        <section className="dashboard-grid">
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

export default App
