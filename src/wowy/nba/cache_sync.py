from __future__ import annotations

import csv
from pathlib import Path
from typing import Callable

from wowy.apps.wowy.derive import WOWY_HEADER, derive_wowy_games, write_wowy_games_csv
from wowy.data.game_cache_db import (
    ensure_explicit_regular_season_copy,
    import_team_season_csv_cache_into_db,
    load_cache_load_row,
    load_normalized_game_players_from_db,
    load_normalized_games_from_db,
)
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    write_team_season_games_csv,
)
from wowy.nba.paths import (
    legacy_regular_season_filename,
    normalized_game_players_path,
    normalized_games_path,
    resolve_existing_path,
    wowy_games_path,
)
from wowy.nba.team_seasons import TeamSeasonScope
from wowy.nba.validation import (
    validate_team_season_consistency,
    validate_team_season_records,
)
from wowy.data.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)


LogFn = Callable[[str], None]


def wowy_cache_is_current(
    wowy_path: Path,
    normalized_games_path: Path,
    normalized_game_players_path: Path,
) -> bool:
    if not wowy_path.exists():
        return False

    with open(wowy_path, "r", encoding="utf-8", newline="") as f:
        header = next(csv.reader(f), None)
    if header != WOWY_HEADER:
        return False

    wowy_mtime = wowy_path.stat().st_mtime
    return (
        wowy_mtime >= normalized_games_path.stat().st_mtime
        and wowy_mtime >= normalized_game_players_path.stat().st_mtime
    )


def rebuild_wowy_for_team_season(
    team_season: TeamSeasonScope,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    season_type: str = "Regular Season",
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> Path:
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
    output_path = wowy_games_path(team_season, wowy_output_dir, season_type)

    games, game_players, loaded_from_db = load_team_season_normalized_records(
        team_season,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
    )
    derived_games = derive_wowy_games(games, game_players)
    write_wowy_games_csv(output_path, derived_games)
    if season_type == "Regular Season":
        ensure_explicit_regular_season_copy(
            output_path,
            output_path.with_name(legacy_regular_season_filename(team_season)),
        )
    if loaded_from_db:
        consistency = validate_team_season_records(
            games,
            game_players,
            derived_games,
        )
    else:
        consistency = validate_team_season_consistency(
            team=team_season.team,
            season=team_season.season,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            season_type=season_type,
        )
    if consistency != "ok":
        raise ValueError(
            f"Inconsistent team-season cache for {team_season.team} {team_season.season}: {consistency}"
        )
    return output_path


def ensure_team_season_data(
    team_season: TeamSeasonScope,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
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
    wowy_path = resolve_existing_path(
        team_season,
        wowy_output_dir,
        season_type,
    ) or wowy_games_path(team_season, wowy_output_dir, season_type)

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
            csv_path=wowy_path,
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
    if season_type == "Regular Season":
        ensure_explicit_regular_season_copy(
            games_path,
            normalized_games_path(team_season, normalized_games_input_dir, season_type),
        )
        ensure_explicit_regular_season_copy(
            game_players_path,
            normalized_game_players_path(
                team_season,
                normalized_game_players_input_dir,
                season_type,
            ),
        )
        ensure_explicit_regular_season_copy(
            wowy_path,
            wowy_games_path(team_season, wowy_output_dir, season_type),
        )

    consistency = (
        validate_team_season_consistency(
            team=team_season.team,
            season=team_season.season,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            season_type=season_type,
        )
        if wowy_path.exists()
        else "missing"
    )

    if (
        not wowy_cache_is_current(wowy_path, games_path, game_players_path)
        or consistency != "ok"
    ):
        if log is not None:
            reason = "stale" if consistency == "ok" else consistency
            log(f"rebuild {team_season.team} {team_season.season} reason={reason}")
        rebuild_wowy_for_team_season(
            team_season=team_season,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            season_type=season_type,
            player_metrics_db_path=player_metrics_db_path,
        )


def load_team_season_normalized_records(
    team_season: TeamSeasonScope,
    *,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    season_type: str,
    player_metrics_db_path: Path,
) -> tuple[list, list, bool]:
    games = load_normalized_games_from_db(
        player_metrics_db_path,
        season_type=season_type,
        teams=[team_season.team],
        seasons=[team_season.season],
    )
    game_players = load_normalized_game_players_from_db(
        player_metrics_db_path,
        season_type=season_type,
        teams=[team_season.team],
        seasons=[team_season.season],
    )
    if games and game_players:
        allowed_game_teams = {(game.game_id, game.team) for game in games}
        return (
            games,
            [
                player
                for player in game_players
                if (player.game_id, player.team) in allowed_game_teams
            ],
            True,
        )

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
    return (
        load_normalized_games_from_csv(games_path),
        load_normalized_game_players_from_csv(game_players_path),
        False,
    )
