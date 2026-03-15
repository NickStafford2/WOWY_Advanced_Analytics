from __future__ import annotations

from pathlib import Path

from wowy.apps.wowy.derive import WOWY_HEADER
from wowy.data.combine import combine_csv_paths, combine_normalized_files
from wowy.nba.cache_sync import ensure_team_season_data
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    load_player_names_from_cache,
)
from wowy.nba.paths import (
    normalized_game_players_path,
    normalized_games_path,
    wowy_games_path,
)
from wowy.nba.team_seasons import TeamSeasonScope, resolve_team_seasons
from wowy.normalized_io import load_normalized_games_from_csv


def prepare_wowy_inputs(
    teams: list[str] | None,
    seasons: list[str] | None,
    combined_wowy_csv: Path,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    log=print,
) -> tuple[Path, dict[int, str]]:
    team_seasons = resolve_team_seasons(teams, seasons, normalized_games_input_dir)
    if not team_seasons:
        raise ValueError("No cached data matched the requested WOWY scope")

    for team_season in team_seasons:
        ensure_team_season_data(
            team_season=team_season,
            season_type=season_type,
            source_data_dir=source_data_dir,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            log=log,
        )

    combine_csv_paths(
        [wowy_games_path(team_season, wowy_output_dir) for team_season in team_seasons],
        combined_wowy_csv,
        WOWY_HEADER,
    )
    return combined_wowy_csv, load_player_names_from_cache(source_data_dir)


def prepare_regression_inputs(
    teams: list[str] | None,
    seasons: list[str] | None,
    combined_games_csv: Path,
    combined_game_players_csv: Path,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    log=print,
) -> tuple[Path, Path]:
    team_seasons = resolve_team_seasons(teams, seasons, normalized_games_input_dir)
    if not team_seasons:
        raise ValueError("No cached data matched the requested regression scope")

    requested_team_seasons = list(team_seasons)

    for team_season in team_seasons:
        ensure_team_season_data(
            team_season=team_season,
            season_type=season_type,
            source_data_dir=source_data_dir,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            log=log,
        )

    if teams:
        opponent_team_seasons: set[TeamSeasonScope] = set()
        for team_season in requested_team_seasons:
            games = load_normalized_games_from_csv(
                normalized_games_path(team_season, normalized_games_input_dir)
            )
            for game in games:
                opponent_team_seasons.add(
                    TeamSeasonScope(team=game.opponent, season=game.season)
                )

        for team_season in sorted(opponent_team_seasons):
            if team_season in requested_team_seasons:
                continue
            ensure_team_season_data(
                team_season=team_season,
                season_type=season_type,
                source_data_dir=source_data_dir,
                normalized_games_input_dir=normalized_games_input_dir,
                normalized_game_players_input_dir=normalized_game_players_input_dir,
                wowy_output_dir=wowy_output_dir,
                log=log,
            )
            team_seasons.append(team_season)

    combine_normalized_files(
        games_input_paths=[
            normalized_games_path(team_season, normalized_games_input_dir)
            for team_season in team_seasons
        ],
        game_players_input_paths=[
            normalized_game_players_path(
                team_season, normalized_game_players_input_dir
            )
            for team_season in team_seasons
        ],
        games_output_path=combined_games_csv,
        game_players_output_path=combined_game_players_csv,
    )
    return combined_games_csv, combined_game_players_csv
