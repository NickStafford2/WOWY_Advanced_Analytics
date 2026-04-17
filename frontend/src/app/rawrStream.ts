import type {
  LeaderboardErrorEvent,
  LeaderboardProgressEvent,
  LeaderboardResultEvent,
} from './loadingTypes'

export function streamRawrLeaderboard<TPayload>({
  url,
  onStarted,
  onProgress,
  onResult,
  onError,
}: {
  url: string
  onStarted?: (payload: unknown) => void
  onProgress: (progress: LeaderboardProgressEvent) => void
  onResult: (payload: TPayload) => void
  onError: (error: string) => void
}) {
  const source = new EventSource(url)

  source.addEventListener('started', (event) => {
    onStarted?.(JSON.parse((event as MessageEvent).data))
  })

  source.addEventListener('progress', (event) => {
    const payload = JSON.parse((event as MessageEvent).data) as LeaderboardProgressEvent
    onProgress(payload)
  })

  source.addEventListener('result', (event) => {
    const payload = JSON.parse((event as MessageEvent).data) as LeaderboardResultEvent<TPayload>
    onResult(payload.payload)
    source.close()
  })

  source.addEventListener('error', (event) => {
    try {
      const payload = JSON.parse((event as MessageEvent).data) as LeaderboardErrorEvent
      onError(payload.message)
    } catch {
      onError('The RAWR stream disconnected unexpectedly.')
    }
    source.close()
  })

  return {
    close: () => source.close(),
  }
}
