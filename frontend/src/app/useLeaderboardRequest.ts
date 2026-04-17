import { useEffect, useRef, useState, type MutableRefObject } from 'react'
import type { LeaderboardPayload, TeamOption } from './leaderboardApiTypes'
import { buildLeaderboardStreamUrl } from './leaderboardParams'
import { isAllTeamsSelection, filterSelectedTeamIdsForAvailableTeams } from './leaderboardTeams'
import type { LeaderboardFilters } from './leaderboardTypes'
import type { LeaderboardProgressEvent } from './loadingTypes'
import type { MetricId } from './metricTypes'
import { streamLeaderboard } from './leaderboardStream'

type StreamHandle = {
  close: () => void
}

type RunLeaderboardRequestArgs = {
  metric: MetricId
  filters: LeaderboardFilters
  availableSeasons: string[]
  availableTeams: TeamOption[]
  restartLoadingClock: () => void
}

type UseLeaderboardRequestValue = {
  leaderboard: LeaderboardPayload | null
  error: string
  isLoading: boolean
  serverProgress: LeaderboardProgressEvent | null
  clearRequestState: () => void
  setRequestError: (message: string) => void
  runRequest: (args: RunLeaderboardRequestArgs) => Promise<LeaderboardPayload | null>
}

export function useLeaderboardRequest({
  metricRef,
}: {
  metricRef: MutableRefObject<MetricId>
}): UseLeaderboardRequestValue {
  const streamRef = useRef<StreamHandle | null>(null)

  const [leaderboard, setLeaderboard] = useState<LeaderboardPayload | null>(null)
  const [error, setError] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [serverProgress, setServerProgress] = useState<LeaderboardProgressEvent | null>(null)

  useEffect(() => {
    return () => {
      streamRef.current?.close()
    }
  }, [])

  function clearRequestState(): void {
    streamRef.current?.close()
    streamRef.current = null
    setLeaderboard(null)
    setError('')
    setIsLoading(false)
    setServerProgress(null)
  }

  async function runRequest({
    metric,
    filters,
    availableSeasons,
    availableTeams,
    restartLoadingClock,
  }: RunLeaderboardRequestArgs): Promise<LeaderboardPayload | null> {
    streamRef.current?.close()
    streamRef.current = null

    const selectedTeamIds = filterSelectedTeamIdsForAvailableTeams(filters.teamIds, availableTeams)
    const hasSelectedTeams =
      availableTeams.length > 0 &&
      (isAllTeamsSelection(filters.teamIds, availableTeams) || selectedTeamIds.length > 0)

    console.log('[Leaderboard] request start', {
      metric,
      seasonCount: availableSeasons.length,
      teamCount: availableTeams.length,
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

    const streamUrl = buildLeaderboardStreamUrl({
      metric,
      filters,
      availableSeasons,
      availableTeams,
    })

    console.log('[Leaderboard] USING SSE STREAM', { streamUrl })

    return await new Promise<LeaderboardPayload | null>((resolve) => {
      let finished = false

      const finish = (value: LeaderboardPayload | null) => {
        if (finished) {
          return
        }
        finished = true
        streamRef.current = null
        setIsLoading(false)
        console.log('[Leaderboard] stream finished', { hasPayload: value != null })
        resolve(value)
      }

      streamRef.current = streamLeaderboard<LeaderboardPayload>({
        url: streamUrl,
        onStarted: (payload) => {
          console.log('[Leaderboard] stream started', payload)
        },
        onProgress: (progress) => {
          console.log('[Leaderboard] stream progress', progress)
          if (metricRef.current !== metric) {
            return
          }
          setServerProgress(progress)
        },
        onResult: (payload) => {
          console.log('[Leaderboard] stream result', payload)
          if (metricRef.current !== metric) {
            finish(null)
            return
          }
          setLeaderboard(payload)
          finish(payload)
        },
        onError: (message) => {
          console.error('[Leaderboard] stream error', message)
          if (metricRef.current !== metric) {
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

  return {
    leaderboard,
    error,
    isLoading,
    serverProgress,
    clearRequestState,
    setRequestError: setError,
    runRequest,
  }
}
