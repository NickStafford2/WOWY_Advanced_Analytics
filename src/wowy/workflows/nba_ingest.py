from __future__ import annotations

from pathlib import Path
from typing import Callable

from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest import DEFAULT_SOURCE_DATA_DIR
from wowy.nba.ingest.runner import (
    ProgressFn,
    cache_team_season_data,
    fetch_team_season_data,
    ingest_team_season,
)


def run_team_season_ingest(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    *,
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
    cached_only: bool = False,
):
    return cache_team_season_data(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=player_metrics_db_path,
        log=log,
        progress=progress,
        cached_only=cached_only,
    )


__all__ = [
    "fetch_team_season_data",
    "ingest_team_season",
    "run_team_season_ingest",
]
