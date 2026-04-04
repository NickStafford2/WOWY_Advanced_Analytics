from __future__ import annotations

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._metric_handlers import MetricWebHandlers
from rawr_analytics.web._rawr_handlers import build_metric_handlers as build_rawr_metric_handlers
from rawr_analytics.web._wowy_handlers import build_metric_handlers as build_wowy_metric_handlers

_METRIC_HANDLERS: dict[Metric, MetricWebHandlers] = {
    Metric.RAWR: build_rawr_metric_handlers(),
    Metric.WOWY: build_wowy_metric_handlers(Metric.WOWY),
    Metric.WOWY_SHRUNK: build_wowy_metric_handlers(Metric.WOWY_SHRUNK),
}


def get_metric_handlers(metric: Metric) -> MetricWebHandlers:
    handlers = _METRIC_HANDLERS.get(metric)
    if handlers is None:
        raise ValueError(f"Unsupported web metric: {metric}")
    return handlers
