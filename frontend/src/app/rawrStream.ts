import type {
  LeaderboardErrorEvent,
  LeaderboardProgressEvent,
  LeaderboardResultEvent,
} from './loadingTypes'

type StreamHandle = {
  close: () => void
}

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
    console.log('[RAWR SSE] open', { url })
  })

  source.addEventListener('started', (event) => {
    const payload = JSON.parse((event as MessageEvent).data)
    console.log('[RAWR SSE] started', payload)
    onStarted?.(payload)
  })

  source.addEventListener('progress', (event) => {
    const payload = JSON.parse((event as MessageEvent).data) as LeaderboardProgressEvent
    console.log('[RAWR SSE] progress', payload)
    onProgress(payload)
  })

  source.addEventListener('result', (event) => {
    const payload = JSON.parse((event as MessageEvent).data) as LeaderboardResultEvent<TPayload>
    console.log('[RAWR SSE] result', payload)
    hasReceivedResult = true
    onResult(payload.payload)
    close()
  })

  source.addEventListener('error', (event) => {
    const messageEvent = event as MessageEvent<string>

    if (typeof messageEvent.data === 'string' && messageEvent.data.length > 0) {
      try {
        const payload = JSON.parse(messageEvent.data) as LeaderboardErrorEvent
        console.error('[RAWR SSE] server error', payload)
        onError(payload.message)
        close()
        return
      } catch {
        console.error('[RAWR SSE] malformed server error payload', messageEvent.data)
      }
    }

    if (hasReceivedResult || source.readyState === EventSource.CLOSED) {
      console.log('[RAWR SSE] stream closed')
      close()
      return
    }

    console.warn('[RAWR SSE] transport error', {
      readyState: source.readyState,
      event,
    })
  })

  return { close }
}
