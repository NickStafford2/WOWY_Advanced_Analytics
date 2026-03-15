from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from nba_api.stats.static import teams as nba_teams

from wowy.apps.wowy.derive import WOWY_HEADER, derive_wowy_games, write_wowy_games_csv
from wowy.combine_games_cli import combine_csv_paths, combine_normalized_files
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    load_player_names_from_cache,
    write_team_season_games_csv,
)
from wowy.nba.validation import validate_team_season_consistency
from wowy.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)


LogFn = Callable[[str], None]


@dataclass(frozen=True, order=True)
class TeamSeasonScope:
    team: str
    season: str


def parse_team_season_filename(path: Path) -> TeamSeasonScope:
    team, separator, season = path.stem.partition("_")
    if not separator or not team or not season:
        raise ValueError(
            f"Unexpected team-season filename {path.name!r}. Expected TEAM_SEASON.csv."
        )
    return TeamSeasonScope(team=team.upper(), season=season)


def list_cached_team_seasons(
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
) -> list[TeamSeasonScope]:
    return sorted(
        parse_team_season_filename(path)
        for path in normalized_games_input_dir.glob("*.csv")
    )


def resolve_team_seasons(
    teams: list[str] | None,
    seasons: list[str] | None,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
) -> list[TeamSeasonScope]:
    normalized_teams = [team.upper() for team in teams] if teams else None
    cached_team_seasons = list_cached_team_seasons(normalized_games_input_dir)

    if seasons:
        target_teams = normalized_teams or sorted(
            team["abbreviation"] for team in nba_teams.get_teams()
        )
        return sorted(
            TeamSeasonScope(team=team, season=season)
            for season in seasons
            for team in target_teams
        )

    if normalized_teams:
        return [
            team_season
            for team_season in cached_team_seasons
            if team_season.team in normalized_teams
        ]

    return cached_team_seasons


def normalized_games_path(
    team_season: TeamSeasonScope,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
) -> Path:
    return normalized_games_input_dir / f"{team_season.team}_{team_season.season}.csv"


def normalized_game_players_path(
    team_season: TeamSeasonScope,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
) -> Path:
    return (
        normalized_game_players_input_dir
        / f"{team_season.team}_{team_season.season}.csv"
    )


def wowy_games_path(
    team_season: TeamSeasonScope,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
) -> Path:
    return wowy_output_dir / f"{team_season.team}_{team_season.season}.csv"


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


def prepare_wowy_inputs(
    teams: list[str] | None,
    seasons: list[str] | None,
    combined_wowy_csv: Path,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    log: LogFn | None = print,
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
    log: LogFn | None = print,
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
