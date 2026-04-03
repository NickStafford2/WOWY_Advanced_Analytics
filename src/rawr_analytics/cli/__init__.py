"""Public CLI interface for command entrypoints and CLI request builders."""

from rawr_analytics.cli._requests import (
    build_metric_store_refresh_request,
    build_rebuild_request,
)
from rawr_analytics.cli.cache_season_data import main as cache_season_data_main
from rawr_analytics.cli.cache_season_data import run as cache_season_data_run
from rawr_analytics.cli.rebuild_player_metrics_db import (
    build_parser as build_rebuild_player_metrics_db_parser,
)
from rawr_analytics.cli.rebuild_player_metrics_db import (
    main as rebuild_player_metrics_db_main,
)
from rawr_analytics.cli.rebuild_player_metrics_db import (
    run as rebuild_player_metrics_db_run,
)

__all__ = [
    "build_metric_store_refresh_request",
    "build_rebuild_player_metrics_db_parser",
    "build_rebuild_request",
    "cache_season_data_main",
    "cache_season_data_run",
    "rebuild_player_metrics_db_main",
    "rebuild_player_metrics_db_run",
]
