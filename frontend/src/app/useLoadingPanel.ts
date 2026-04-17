import { useEffect, useMemo, useState } from 'react'
import { buildLoadingPanelModel } from './loading'
import type { LeaderboardProgressEvent } from './loadingTypes'
import type { MetricId } from './metricTypes'

type UseLoadingPanelArgs = {
  metric: MetricId
  metricLabel: string
  isBootstrapping: boolean
  isLoading: boolean
  serverProgress: LeaderboardProgressEvent | null
}

type UseLoadingPanelValue = {
  loadingPanel: ReturnType<typeof buildLoadingPanelModel>
  restartLoadingClock: () => void
}

export function useLoadingPanel({
  metric,
  metricLabel,
  isBootstrapping,
  isLoading,
  serverProgress,
}: UseLoadingPanelArgs): UseLoadingPanelValue {
  const [loadingStartedAt, setLoadingStartedAt] = useState<number>(() => Date.now())
  const [elapsedMs, setElapsedMs] = useState(0)

  function restartLoadingClock(): void {
    const now = Date.now()
    setLoadingStartedAt(now)
    setElapsedMs(0)
  }

  useEffect(() => {
    if (!isLoading && !isBootstrapping) {
      return
    }

    const intervalId = window.setInterval(() => {
      setElapsedMs(Date.now() - loadingStartedAt)
    }, 100)

    return () => {
      window.clearInterval(intervalId)
    }
  }, [isBootstrapping, isLoading, loadingStartedAt])

  const loadingPanel = useMemo(
    () =>
      buildLoadingPanelModel({
        metric,
        metricLabel,
        isBootstrapping,
        elapsedMs,
        serverProgress,
      }),
    [metric, metricLabel, isBootstrapping, elapsedMs, serverProgress],
  )

  return {
    loadingPanel,
    restartLoadingClock,
  }
}
