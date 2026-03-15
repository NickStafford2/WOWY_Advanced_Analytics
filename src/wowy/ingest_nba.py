from __future__ import annotations

import csv
from pathlib import Path

from nba_api.stats.endpoints import boxscoretraditionalv2, leaguegamefinder
from nba_api.stats.static import teams

from wowy.types import GameRecord


def fetch_team_season_games(
    team_abbreviation: str,
    season: str,
    season_type: str = "Regular Season",
) -> list[GameRecord]:
    """Fetch one NBA team-season and return rows in the existing game CSV shape.

    Each returned record matches the current WOWY input model:
    one row per game from one team's perspective with `game_id`, `team`,
    `margin`, and the set of players who appeared in that game.
    """

    team = teams.find_team_by_abbreviation(team_abbreviation.upper())
    if team is None:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")

    finder = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=team["id"],
        season_nullable=season,
        season_type_nullable=season_type,
    )
    games_df = finder.get_data_frames()[0]

    if games_df.empty:
        return []

    records: list[GameRecord] = []

    for game_id in games_df["GAME_ID"].drop_duplicates().tolist():
        records.append(_fetch_game_record(game_id=game_id, team_abbreviation=team["abbreviation"]))

    return records


def write_team_season_games_csv(
    team_abbreviation: str,
    season: str,
    csv_path: Path | str,
    season_type: str = "Regular Season",
) -> None:
    """Fetch one NBA team-season and write it as the existing `games.csv` format."""

    games = fetch_team_season_games(
        team_abbreviation=team_abbreviation,
        season=season,
        season_type=season_type,
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
                    "players": ";".join(sorted(game["players"])),
                }
            )


def _fetch_game_record(game_id: str, team_abbreviation: str) -> GameRecord:
    """Fetch one NBA game and normalize the selected team into a `GameRecord`."""

    box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    player_stats_df, team_stats_df = box_score.get_data_frames()[:2]

    team_rows = team_stats_df.loc[
        team_stats_df["TEAM_ABBREVIATION"] == team_abbreviation,
    ]
    if team_rows.empty:
        raise ValueError(
            f"Team {team_abbreviation!r} not found in box score for game {game_id!r}"
        )

    player_rows = player_stats_df.loc[
        player_stats_df["TEAM_ABBREVIATION"] == team_abbreviation,
    ]
    players = _extract_players_who_appeared(player_rows["PLAYER_NAME"], player_rows["MIN"])
    if not players:
        raise ValueError(
            f"No active players found for team {team_abbreviation!r} in game {game_id!r}"
        )

    plus_minus = float(team_rows.iloc[0]["PLUS_MINUS"])
    return {
        "game_id": game_id,
        "team": team_abbreviation,
        "margin": plus_minus,
        "players": players,
    }


def _extract_players_who_appeared(player_names, minutes_played) -> set[str]:
    """Return the players who logged non-zero minutes in the game."""

    players: set[str] = set()

    for player_name, minutes in zip(player_names.tolist(), minutes_played.tolist(), strict=True):
        if not player_name:
            continue
        if not _played_in_game(minutes):
            continue
        players.add(str(player_name).strip())

    return players


def _played_in_game(minutes: object) -> bool:
    """Return whether the NBA box score minute value indicates game participation."""

    if minutes is None:
        return False

    minute_text = str(minutes).strip()
    if not minute_text:
        return False
    if minute_text in {"0", "0:00", "0.0"}:
        return False

    return True
