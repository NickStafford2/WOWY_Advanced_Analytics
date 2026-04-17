import type {
  LeaderboardErrorEvent,
  LeaderboardProgressEvent,
  LeaderboardResultEvent,
} from './loadingTypes'

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
    console.log('[Leaderboard SSE] open', { url })
  })

  source.addEventListener('started', (event) => {
    const payload = JSON.parse((event as MessageEvent).data)
    console.log('[Leaderboard SSE] started', payload)
    onStarted?.(payload)
  })

  source.addEventListener('progress', (event) => {
    const payload = JSON.parse((event as MessageEvent).data) as LeaderboardProgressEvent
    console.log('[Leaderboard SSE] progress', payload)
    onProgress(payload)
  })

  source.addEventListener('result', (event) => {
    const payload = JSON.parse((event as MessageEvent).data) as LeaderboardResultEvent<TPayload>
    console.log('[Leaderboard SSE] result', payload)
    hasReceivedResult = true
    onResult(payload.payload)
    close()
  })

  source.addEventListener('error', (event) => {
    const messageEvent = event as MessageEvent<string>

    if (typeof messageEvent.data === 'string' && messageEvent.data.length > 0) {
      try {
        const payload = JSON.parse(messageEvent.data) as LeaderboardErrorEvent
        console.error('[Leaderboard SSE] server error', payload)
        onError(payload.message)
        close()
        return
      } catch {
        console.error('[Leaderboard SSE] malformed server error payload', messageEvent.data)
      }
    }

    if (hasReceivedResult || source.readyState === EventSource.CLOSED) {
      console.log('[Leaderboard SSE] stream closed')
      close()
      return
    }

    console.warn('[Leaderboard SSE] transport error', {
      readyState: source.readyState,
      event,
    })
  })

  return { close }
}
