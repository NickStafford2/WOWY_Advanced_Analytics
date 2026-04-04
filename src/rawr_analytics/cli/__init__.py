"""Public CLI interface for command entrypoints."""

from rawr_analytics.cli.download_kaggle import main as download_kaggle_main
from rawr_analytics.cli.ingest_nba_api import main as ingest_nba_api_main
from rawr_analytics.cli.rawr import main as rawr_main
from rawr_analytics.cli.rebuild_player_metrics_db import (
    main as rebuild_player_metrics_db_main,
)
from rawr_analytics.cli.run_web import main as run_web_main
from rawr_analytics.cli.run_web import run as run_web_run
from rawr_analytics.cli.wowy import main as wowy_main
from rawr_analytics.cli.wowy import run as wowy_run

__all__ = [
    "download_kaggle_main",
    "ingest_nba_api_main",
    "rawr_main",
    "rebuild_player_metrics_db_main",
    "run_web_main",
    "run_web_run",
    "wowy_main",
    "wowy_run",
]
