import type {
  LeaderboardErrorEvent,
  LeaderboardProgressEvent,
  LeaderboardResultEvent,
} from './loadingTypes'
import { debugLeaderboard } from './leaderboardDebug'

type StreamHandle = {
  close: () => void
}

export function streamLeaderboard<TPayload>({
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
}): StreamHandle {
  const source = new EventSource(url)
  let isClosed = false
  let hasReceivedResult = false

  const close = () => {
    if (isClosed) {
      return
    }
    isClosed = true
    source.close()
  }

  source.addEventListener('open', () => {
    debugLeaderboard('SSE', 'open', { url })
  })

  source.addEventListener('started', (event) => {
    const payload = JSON.parse((event as MessageEvent).data)
    debugLeaderboard('SSE', 'started', payload)
    onStarted?.(payload)
  })

  source.addEventListener('progress', (event) => {
    const payload = JSON.parse((event as MessageEvent).data) as LeaderboardProgressEvent
    debugLeaderboard('SSE', 'progress', payload)
    onProgress(payload)
  })

  source.addEventListener('result', (event) => {
    const payload = JSON.parse((event as MessageEvent).data) as LeaderboardResultEvent<TPayload>
    debugLeaderboard('SSE', 'result', payload)
    hasReceivedResult = true
    onResult(payload.payload)
    close()
  })

  source.addEventListener('error', (event) => {
    const messageEvent = event as MessageEvent<string>

    if (typeof messageEvent.data === 'string' && messageEvent.data.length > 0) {
      try {
        const payload = JSON.parse(messageEvent.data) as LeaderboardErrorEvent
        debugLeaderboard('SSE', 'server error', payload, 'error')
        onError(payload.message)
        close()
        return
      } catch {
        debugLeaderboard('SSE', 'malformed server error payload', messageEvent.data, 'error')
      }
    }

    if (hasReceivedResult || source.readyState === EventSource.CLOSED) {
      debugLeaderboard('SSE', 'stream closed')
      close()
      return
    }

    debugLeaderboard('SSE', 'transport error', {
      readyState: source.readyState,
      event,
    }, 'warn')
  })

  return { close }
}
