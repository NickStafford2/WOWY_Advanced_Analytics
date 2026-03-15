from __future__ import annotations

import csv
from pathlib import Path

from nba_api.stats.static import teams

from wowy.nba_cache import DEFAULT_SOURCE_DATA_DIR, load_or_fetch_league_games
from wowy.nba_normalize import (
    fetch_game_record,
    load_player_names_from_cache as load_cached_player_names,
    result_set_to_data_frame,
)
from wowy.types import GameRecord


def fetch_team_season_games(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
) -> list[GameRecord]:
    """Fetch one NBA team-season and return rows in the existing game CSV shape.

    Each returned record matches the current WOWY input model:
    one row per game from one team's perspective with `game_id`, `team`,
    `margin`, and the set of NBA player ids who appeared in that game.
    """

    team = teams.find_team_by_abbreviation(team_abbreviation.upper())
    if team is None:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")

    finder_payload = _load_or_fetch_league_games(
        team["id"],
        team["abbreviation"],
        season,
        season_type,
        source_data_dir,
    )
    games_df = result_set_to_data_frame(finder_payload["resultSets"][0])

    if games_df.empty:
        return []

    records: list[GameRecord] = []

    for game_id in games_df["GAME_ID"].drop_duplicates().tolist():
        records.append(
            fetch_game_record(game_id, team["abbreviation"], source_data_dir)
        )

    return records


def write_team_season_games_csv(
    team_abbreviation: str,
    season: str,
    csv_path: Path | str,
    season_type: str = "Regular Season",
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
) -> None:
    """Fetch one NBA team-season and write it as the existing `games.csv` format."""

    games = fetch_team_season_games(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
    )

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["game_id", "team", "margin", "players"])
        writer.writeheader()

        for game in games:
            writer.writerow(
                {
                    "game_id": game["game_id"],
                    "team": game["team"],
                    "margin": game["margin"],
                    "players": ";".join(
                        str(player_id) for player_id in sorted(game["players"])
                    ),
                }
            )


def load_player_names_from_cache(
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
) -> dict[int, str]:
    """Load a player-id-to-name mapping from cached NBA box score payloads."""

    return load_cached_player_names(source_data_dir)


def _load_or_fetch_league_games(
    team_id: int,
    team_abbreviation: str,
    season: str,
    season_type: str,
    source_data_dir: Path,
) -> dict:
    """Load a cached team-season response or fetch and cache it from the NBA API."""

    return load_or_fetch_league_games(
        team_id=team_id,
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
        source_data_dir=source_data_dir,
    )
