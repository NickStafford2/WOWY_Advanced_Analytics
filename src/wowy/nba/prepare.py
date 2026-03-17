from __future__ import annotations

from pathlib import Path

from wowy.apps.wowy.derive import WOWY_HEADER
from wowy.apps.wowy.derive import derive_wowy_games
from wowy.apps.wowy.models import WowyGameRecord
from wowy.data.combine import combine_csv_paths, combine_normalized_files
from wowy.data.game_cache_db import (
    load_cache_load_row,
    load_normalized_game_players_from_db,
    load_normalized_games_from_db,
)
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
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
    resolve_existing_path,
    wowy_games_path,
)
from wowy.nba.team_seasons import TeamSeasonScope, resolve_team_seasons
from wowy.data.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord


def prepare_wowy_inputs(
    teams: list[str] | None,
    seasons: list[str] | None,
    combined_wowy_csv: Path,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log=print,
) -> tuple[Path, dict[int, str]]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        normalized_games_input_dir,
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )
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
            player_metrics_db_path=player_metrics_db_path,
            log=log,
        )

    combine_csv_paths(
        [
            wowy_games_path(team_season, wowy_output_dir, season_type)
            for team_season in team_seasons
        ],
        combined_wowy_csv,
        WOWY_HEADER,
    )
    return combined_wowy_csv, load_player_names_from_cache(source_data_dir)


def prepare_rawr_inputs(
    teams: list[str] | None,
    seasons: list[str] | None,
    combined_games_csv: Path,
    combined_game_players_csv: Path,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log=print,
) -> tuple[Path, Path]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        normalized_games_input_dir,
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )
    if not team_seasons:
        raise ValueError("No cached data matched the requested RAWR scope")

    requested_team_seasons = list(team_seasons)

    for team_season in team_seasons:
        ensure_team_season_data(
            team_season=team_season,
            season_type=season_type,
            source_data_dir=source_data_dir,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            player_metrics_db_path=player_metrics_db_path,
            log=log,
        )

    if teams:
        opponent_team_seasons: set[TeamSeasonScope] = set()
        for team_season in requested_team_seasons:
            games = load_normalized_games_from_csv(
                normalized_games_path(
                    team_season,
                    normalized_games_input_dir,
                    season_type,
                )
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
                player_metrics_db_path=player_metrics_db_path,
                log=log,
            )
            team_seasons.append(team_season)

    combine_normalized_files(
        games_input_paths=[
            normalized_games_path(
                team_season,
                normalized_games_input_dir,
                season_type,
            )
            for team_season in team_seasons
        ],
        game_players_input_paths=[
            normalized_game_players_path(
                team_season,
                normalized_game_players_input_dir,
                season_type,
            )
            for team_season in team_seasons
        ],
        games_output_path=combined_games_csv,
        game_players_output_path=combined_game_players_csv,
    )
    return combined_games_csv, combined_game_players_csv


def prepare_wowy_game_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    log=print,
) -> tuple[list[WowyGameRecord], dict[int, str]]:
    games, game_players = prepare_normalized_scope_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
        log=log,
    )
    return derive_wowy_games(games, game_players), load_player_names_from_cache(source_data_dir)


def prepare_normalized_scope_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    include_opponents_for_team_scope: bool = True,
    log=print,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        normalized_games_input_dir,
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")

    requested_team_seasons = list(team_seasons)
    for team_season in requested_team_seasons:
        _ensure_team_season_scope_available(
            team_season,
            season_type=season_type,
            source_data_dir=source_data_dir,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            player_metrics_db_path=player_metrics_db_path,
            log=log,
        )

    if teams and include_opponents_for_team_scope:
        opponent_team_seasons = {
            TeamSeasonScope(team=game.opponent, season=game.season)
            for game in _load_games_from_db_or_csv(
                requested_team_seasons,
                normalized_games_input_dir=normalized_games_input_dir,
                season_type=season_type,
                player_metrics_db_path=player_metrics_db_path,
            )
        }
        for team_season in sorted(opponent_team_seasons):
            if team_season in requested_team_seasons:
                continue
            _ensure_team_season_scope_available(
                team_season,
                season_type=season_type,
                source_data_dir=source_data_dir,
                normalized_games_input_dir=normalized_games_input_dir,
                normalized_game_players_input_dir=normalized_game_players_input_dir,
                wowy_output_dir=wowy_output_dir,
                player_metrics_db_path=player_metrics_db_path,
                log=log,
            )
            team_seasons.append(team_season)

    games = load_normalized_games_from_db(
        player_metrics_db_path,
        season_type=season_type,
        teams=[team_season.team for team_season in team_seasons],
        seasons=sorted({team_season.season for team_season in team_seasons}),
    )
    game_players = load_normalized_game_players_from_db(
        player_metrics_db_path,
        season_type=season_type,
        teams=[team_season.team for team_season in team_seasons],
        seasons=sorted({team_season.season for team_season in team_seasons}),
    )
    games, game_players = _filter_records_to_team_seasons(
        games,
        game_players,
        team_seasons,
    )
    if games and game_players:
        return games, game_players

    games = []
    game_players = []
    for team_season in team_seasons:
        games_path = resolve_existing_path(
            team_season,
            normalized_games_input_dir,
            season_type,
        )
        game_players_path = resolve_existing_path(
            team_season,
            normalized_game_players_input_dir,
            season_type,
        )
        if games_path is None or game_players_path is None:
            continue
        games.extend(load_normalized_games_from_csv(games_path))
        game_players.extend(load_normalized_game_players_from_csv(game_players_path))
    return games, game_players


def _load_games_from_db_or_csv(
    team_seasons: list[TeamSeasonScope],
    *,
    normalized_games_input_dir: Path,
    season_type: str,
    player_metrics_db_path: Path,
) -> list[NormalizedGameRecord]:
    games = load_normalized_games_from_db(
        player_metrics_db_path,
        season_type=season_type,
        teams=[team_season.team for team_season in team_seasons],
        seasons=sorted({team_season.season for team_season in team_seasons}),
    )
    games, _game_players = _filter_records_to_team_seasons(
        games,
        [],
        team_seasons,
    )
    if games:
        return games
    fallback_games: list[NormalizedGameRecord] = []
    for team_season in team_seasons:
        games_path = resolve_existing_path(
            team_season,
            normalized_games_input_dir,
            season_type,
        )
        if games_path is None:
            continue
        fallback_games.extend(load_normalized_games_from_csv(games_path))
    return fallback_games


def _filter_records_to_team_seasons(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    team_seasons: list[TeamSeasonScope],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    allowed_team_seasons = {
        (team_season.team, team_season.season)
        for team_season in team_seasons
    }
    filtered_games = [
        game
        for game in games
        if (game.team, game.season) in allowed_team_seasons
    ]
    allowed_game_teams = {(game.game_id, game.team) for game in filtered_games}
    filtered_game_players = [
        player
        for player in game_players
        if (player.game_id, player.team) in allowed_game_teams
    ]
    return filtered_games, filtered_game_players


def _ensure_team_season_scope_available(
    team_season: TeamSeasonScope,
    *,
    season_type: str,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    player_metrics_db_path: Path,
    log,
) -> None:
    cache_load_row = load_cache_load_row(
        player_metrics_db_path,
        team=team_season.team,
        season=team_season.season,
        season_type=season_type,
    )
    if (
        cache_load_row is not None
        and cache_load_row.games_row_count > 0
        and cache_load_row.game_players_row_count > 0
    ):
        return
    ensure_team_season_data(
        team_season=team_season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        player_metrics_db_path=player_metrics_db_path,
        log=log,
    )
