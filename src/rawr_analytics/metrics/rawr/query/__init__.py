"""RAWR query workflow.

Inputs are normalized query filters from CLI or web request parsing. Outputs are
resolved RAWR player-season records, JSON-ready payloads, and export rows.
"""

from rawr_analytics.metrics.rawr.query.request import RawrQuery, build_rawr_query
from rawr_analytics.metrics.rawr.query.service import (
    MetricQueryExport,
    RawrProgressFn,
    RawrResultSource,
    ResolvedRawrResultDTO,
    build_rawr_leaderboard_export,
    build_rawr_leaderboard_payload,
    build_rawr_options_payload,
    build_rawr_player_seasons_payload,
    build_rawr_span_chart_payload,
    resolve_rawr_result,
)

__all__ = [
    "MetricQueryExport",
    "RawrProgressFn",
    "RawrQuery",
    "RawrResultSource",
    "ResolvedRawrResultDTO",
    "build_rawr_leaderboard_export",
    "build_rawr_leaderboard_payload",
    "build_rawr_options_payload",
    "build_rawr_player_seasons_payload",
    "build_rawr_query",
    "build_rawr_span_chart_payload",
    "resolve_rawr_result",
]
