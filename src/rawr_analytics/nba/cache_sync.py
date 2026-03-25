from __future__ import annotations

from pathlib import Path
from typing import Callable

from wowy.data.game_cache import has_cached_team_season_scope
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.source.cache import DEFAULT_SOURCE_DATA_DIR
from wowy.nba.team_seasons import TeamSeasonScope
from wowy.workflows.nba_ingest import refresh_normalized_team_season_cache

LogFn = Callable[[str], None]


def ensure_team_season_data(
    team_season: TeamSeasonScope,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log: LogFn | None = print,
) -> None:
    if has_cached_team_season_scope(
        player_metrics_db_path,
        team=team_season.team,
        season=team_season.season,
        season_type=season_type,
    ):
        return
    if log is not None:
        log(f"fetch {team_season.team} {team_season.season}")
    refresh_normalized_team_season_cache(
        team_abbreviation=team_season.team,
        season=team_season.season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=player_metrics_db_path,
        log=log,
    )
