from __future__ import annotations

import csv
from pathlib import Path
from typing import Callable

from wowy.apps.wowy.derive import WOWY_HEADER, derive_wowy_games, write_wowy_games_csv
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    write_team_season_games_csv,
)
from wowy.nba.paths import (
    normalized_game_players_path,
    normalized_games_path,
    wowy_games_path,
)
from wowy.nba.team_seasons import TeamSeasonScope
from wowy.nba.validation import validate_team_season_consistency
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
) -> Path:
    games_path = normalized_games_path(team_season, normalized_games_input_dir)
    game_players_path = normalized_game_players_path(
        team_season, normalized_game_players_input_dir
    )
    output_path = wowy_games_path(team_season, wowy_output_dir)

    games = load_normalized_games_from_csv(games_path)
    game_players = load_normalized_game_players_from_csv(game_players_path)
    derived_games = derive_wowy_games(games, game_players)
    write_wowy_games_csv(output_path, derived_games)
    consistency = validate_team_season_consistency(
        team=team_season.team,
        season=team_season.season,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
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
    log: LogFn | None = print,
) -> None:
    games_path = normalized_games_path(team_season, normalized_games_input_dir)
    game_players_path = normalized_game_players_path(
        team_season, normalized_game_players_input_dir
    )
    wowy_path = wowy_games_path(team_season, wowy_output_dir)

    if not games_path.exists() or not game_players_path.exists():
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
            log=log,
        )
        return

    consistency = (
        validate_team_season_consistency(
            team=team_season.team,
            season=team_season.season,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
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
        )
