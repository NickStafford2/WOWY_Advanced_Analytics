import { useEffect, useState } from 'react'
import { buildLoadingPanelModel } from './loading'
import type { AppMode, LoadingPanelModel, MetricId } from './types'

const LOADING_PANEL_DELAY_MS = 250

type UseLoadingPanelOptions = {
  metric: MetricId
  metricLabel: string
  mode: AppMode
  isBootstrapping: boolean
  isLoading: boolean
}

type UseLoadingPanelValue = {
  loadingPanel: LoadingPanelModel | null
  restartLoadingClock: () => void
}

export function useLoadingPanel({
  metric,
  metricLabel,
  mode,
  isBootstrapping,
  isLoading,
}: UseLoadingPanelOptions): UseLoadingPanelValue {
  const [startedAt, setStartedAt] = useState<number | null>(null)
  const [elapsedMs, setElapsedMs] = useState(0)
  const [isDelayComplete, setIsDelayComplete] = useState(false)
  const isActive = isBootstrapping || isLoading

  useEffect(() => {
    if (!isActive || startedAt === null) {
      return
    }

    const timeoutId = window.setTimeout(() => {
      setIsDelayComplete(true)
    }, LOADING_PANEL_DELAY_MS)

    return () => window.clearTimeout(timeoutId)
  }, [isActive, startedAt])

  useEffect(() => {
    if (!isActive || !isDelayComplete || startedAt === null) {
      return
    }

    const intervalId = window.setInterval(() => {
      setElapsedMs(Math.max(Date.now() - startedAt, 0))
    }, 180)

    return () => window.clearInterval(intervalId)
  }, [isActive, isDelayComplete, startedAt])

  const loadingPanel =
    !isActive || !isDelayComplete
      ? null
      : buildLoadingPanelModel({
          metric,
          metricLabel,
          mode,
          isBootstrapping,
          elapsedMs,
        })

  function restartLoadingClock(): void {
    const now = Date.now()
    setStartedAt(now)
    setElapsedMs(0)
    setIsDelayComplete(false)
  }

  return { loadingPanel, restartLoadingClock }
}
