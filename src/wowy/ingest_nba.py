from __future__ import annotations

from pathlib import Path
from typing import Callable

from nba_api.stats.static import teams

from wowy.derive_wowy import derive_wowy_games, write_wowy_games_csv
from wowy.nba_cache import DEFAULT_SOURCE_DATA_DIR, load_or_fetch_league_games
from wowy.normalized_io import (
    write_normalized_game_players_csv,
    write_normalized_games_csv,
)
from wowy.nba_normalize import (
    fetch_normalized_game_data,
    load_player_names_from_cache as load_cached_player_names,
    result_set_to_data_frame,
)
from wowy.types import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
)


DEFAULT_NORMALIZED_GAMES_DIR = Path("data/normalized/nba/games")
DEFAULT_NORMALIZED_GAME_PLAYERS_DIR = Path("data/normalized/nba/game_players")
DEFAULT_WOWY_GAMES_DIR = Path("data/raw/nba/team_games")
ProgressFn = Callable[[dict], None]


def fetch_team_season_data(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    """Fetch one NBA team-season and return canonical normalized game-level records."""

    team = teams.find_team_by_abbreviation(team_abbreviation.upper())
    if team is None:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")

    finder_payload = load_or_fetch_league_games(
        team_id=team["id"],
        team_abbreviation=team["abbreviation"],
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
    )
    games_df = result_set_to_data_frame(finder_payload["resultSets"][0])

    if games_df.empty:
        return [], []

    normalized_games: list[NormalizedGameRecord] = []
    normalized_game_players: list[NormalizedGamePlayerRecord] = []

    unique_games_df = games_df.drop_duplicates(subset=["GAME_ID"])
    total_games = len(unique_games_df)
    for game_index, (_, game_row) in enumerate(unique_games_df.iterrows(), start=1):
        game_id = str(game_row["GAME_ID"])
        try:
            normalized_game, game_players = fetch_normalized_game_data(
                game_id=game_id,
                team_abbreviation=team["abbreviation"],
                season=season,
                game_date=extract_game_date(game_row),
                opponent=extract_opponent(game_row, team["abbreviation"]),
                is_home=extract_is_home(game_row, team["abbreviation"]),
                season_type=season_type,
                source_data_dir=source_data_dir,
                log=log,
            )
        except ValueError as exc:
            if log is not None:
                log(
                    f"skip game {game_id} {team['abbreviation']} {season} reason={exc}"
                )
            if progress is not None:
                progress(
                    {
                        "team": team["abbreviation"],
                        "season": season,
                        "game_id": game_id,
                        "current": game_index,
                        "total": total_games,
                        "status": "skipped",
                    }
                )
            continue

        normalized_games.append(normalized_game)
        normalized_game_players.extend(game_players)
        if progress is not None:
            progress(
                {
                    "team": team["abbreviation"],
                    "season": season,
                    "game_id": game_id,
                    "current": game_index,
                    "total": total_games,
                    "status": "ok",
                }
            )

    return normalized_games, normalized_game_players


def write_team_season_normalized_csvs(
    team_abbreviation: str,
    season: str,
    games_csv_path: Path | str,
    game_players_csv_path: Path | str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    """Fetch one NBA team-season and write canonical normalized CSVs."""

    normalized_games, normalized_game_players = fetch_team_season_data(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
        progress=progress,
    )
    write_normalized_games_csv(games_csv_path, normalized_games)
    write_normalized_game_players_csv(game_players_csv_path, normalized_game_players)
    return normalized_games, normalized_game_players


def write_team_season_games_csv(
    team_abbreviation: str,
    season: str,
    csv_path: Path | str,
    normalized_games_csv_path: Path | str | None = None,
    normalized_game_players_csv_path: Path | str | None = None,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    log: Callable[[str], None] | None = print,
    progress: ProgressFn | None = None,
) -> None:
    """Fetch one NBA team-season and write normalized CSVs plus derived WOWY output."""

    normalized_games_path = Path(
        normalized_games_csv_path
        or DEFAULT_NORMALIZED_GAMES_DIR / f"{team_abbreviation.upper()}_{season}.csv"
    )
    normalized_game_players_path = Path(
        normalized_game_players_csv_path
        or DEFAULT_NORMALIZED_GAME_PLAYERS_DIR / f"{team_abbreviation.upper()}_{season}.csv"
    )

    normalized_games, normalized_game_players = write_team_season_normalized_csvs(
        team_abbreviation=team_abbreviation,
        season=season,
        games_csv_path=normalized_games_path,
        game_players_csv_path=normalized_game_players_path,
        season_type=season_type,
        source_data_dir=source_data_dir,
        log=log,
        progress=progress,
    )
    derived_games = derive_wowy_games(normalized_games, normalized_game_players)
    write_wowy_games_csv(csv_path, derived_games)


def load_player_names_from_cache(
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
) -> dict[int, str]:
    """Load a player-id-to-name mapping from cached NBA box score payloads."""

    return load_cached_player_names(source_data_dir)


def extract_game_date(game_row) -> str:
    game_date = str(game_row.get("GAME_DATE", "")).strip()
    if not game_date:
        raise ValueError(f"Missing GAME_DATE for game {game_row['GAME_ID']!r}")
    return game_date


def extract_opponent(game_row, team_abbreviation: str) -> str:
    matchup = str(game_row.get("MATCHUP", "")).strip()
    if not matchup:
        raise ValueError(f"Missing MATCHUP for game {game_row['GAME_ID']!r}")

    if " vs. " in matchup:
        left, right = matchup.split(" vs. ", maxsplit=1)
        if left == team_abbreviation:
            return right
        if right == team_abbreviation:
            return left
        raise ValueError(f"Unexpected MATCHUP for game {game_row['GAME_ID']!r}: {matchup!r}")
    if " @ " in matchup:
        left, right = matchup.split(" @ ", maxsplit=1)
        if left == team_abbreviation:
            return right
        if right == team_abbreviation:
            return left
        raise ValueError(f"Unexpected MATCHUP for game {game_row['GAME_ID']!r}: {matchup!r}")

    raise ValueError(f"Unsupported MATCHUP for game {game_row['GAME_ID']!r}: {matchup!r}")


def extract_is_home(game_row, team_abbreviation: str) -> bool:
    matchup = str(game_row.get("MATCHUP", "")).strip()
    if " vs. " in matchup:
        left, right = matchup.split(" vs. ", maxsplit=1)
        if left == team_abbreviation:
            return True
        if right == team_abbreviation:
            return False
        raise ValueError(f"Unexpected MATCHUP for game {game_row['GAME_ID']!r}: {matchup!r}")
    if " @ " in matchup:
        left, right = matchup.split(" @ ", maxsplit=1)
        if left == team_abbreviation:
            return False
        if right == team_abbreviation:
            return True
        raise ValueError(f"Unexpected MATCHUP for game {game_row['GAME_ID']!r}: {matchup!r}")
    raise ValueError(f"Unsupported MATCHUP for game {game_row['GAME_ID']!r}: {matchup!r}")
