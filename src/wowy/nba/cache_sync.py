from __future__ import annotations

from pathlib import Path
from typing import Callable

from wowy.data.game_cache_db import (
    import_team_season_csv_cache_into_db,
    load_cache_load_row,
)
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    write_team_season_games_csv,
)
from wowy.nba.paths import (
    normalized_game_players_path,
    normalized_games_path,
    resolve_existing_path,
)
from wowy.nba.team_seasons import TeamSeasonScope
LogFn = Callable[[str], None]


def ensure_team_season_data(
    team_season: TeamSeasonScope,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path | None = None,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log: LogFn | None = print,
) -> None:
    games_path = resolve_existing_path(
        team_season,
        normalized_games_input_dir,
        season_type,
    ) or normalized_games_path(team_season, normalized_games_input_dir, season_type)
    game_players_path = resolve_existing_path(
        team_season,
        normalized_game_players_input_dir,
        season_type,
    ) or normalized_game_players_path(
        team_season,
        normalized_game_players_input_dir,
        season_type,
    )
    cache_load_row = load_cache_load_row(
        player_metrics_db_path,
        team=team_season.team,
        season=team_season.season,
        season_type=season_type,
    )
    has_db_cache = (
        cache_load_row is not None
        and cache_load_row.games_row_count > 0
        and cache_load_row.game_players_row_count > 0
    )

    if (not games_path.exists() or not game_players_path.exists()) and not has_db_cache:
        if log is not None:
            log(f"fetch {team_season.team} {team_season.season}")
        write_team_season_games_csv(
            team_abbreviation=team_season.team,
            season=team_season.season,
            normalized_games_csv_path=games_path,
            normalized_game_players_csv_path=game_players_path,
            season_type=season_type,
            source_data_dir=source_data_dir,
            player_metrics_db_path=player_metrics_db_path,
            log=log,
        )
        return

    if games_path.exists() and game_players_path.exists():
        import_team_season_csv_cache_into_db(
            player_metrics_db_path,
            team_season=team_season,
            season_type=season_type,
            normalized_games_path=games_path,
            normalized_game_players_path=game_players_path,
        )
