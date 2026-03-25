from __future__ import annotations

"""Compatibility module for legacy wowy data imports."""

from wowy.apps.wowy.analysis import DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES
from wowy.apps.wowy.minutes import (
    attach_minute_stats,
    filter_results_by_minutes,
    load_player_minute_stats,
    load_player_season_minute_stats,
)
from wowy.apps.wowy.records import (
    WOWY_METRIC,
    WOWY_SHRUNK_METRIC,
    WowyPlayerSeasonRow,
    available_wowy_seasons,
    build_wowy_metric_rows,
    build_wowy_player_season_records,
    build_wowy_shrunk_metric_rows,
    prepare_wowy_player_season_records,
    serialize_wowy_player_season_records,
)

__all__ = [
    "DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES",
    "WOWY_METRIC",
    "WOWY_SHRUNK_METRIC",
    "WowyPlayerSeasonRow",
    "attach_minute_stats",
    "available_wowy_seasons",
    "build_wowy_metric_rows",
    "build_wowy_player_season_records",
    "build_wowy_shrunk_metric_rows",
    "filter_results_by_minutes",
    "load_player_minute_stats",
    "load_player_season_minute_stats",
    "prepare_wowy_player_season_records",
    "serialize_wowy_player_season_records",
]
