from __future__ import annotations

from pathlib import Path

import pandas as pd

from wowy.nba_cache import load_cached_payload, load_or_fetch_box_score
from wowy.types import GameRecord


def result_set_to_data_frame(result_set: dict) -> pd.DataFrame:
    """Build a pandas DataFrame from an NBA API result set payload."""

    return pd.DataFrame(result_set["rowSet"], columns=result_set["headers"])


def load_player_names_from_cache(source_data_dir: Path) -> dict[int, str]:
    """Load a player-id-to-name mapping from cached NBA box score payloads."""

    player_names: dict[int, str] = {}

    for cache_path in sorted((source_data_dir / "boxscores").glob("*.json")):
        payload = load_cached_payload(cache_path)
        if payload is None:
            continue
        for result_set in payload["resultSets"]:
            headers = result_set["headers"]
            if "PLAYER_ID" not in headers or "PLAYER_NAME" not in headers:
                continue
            player_stats_df = result_set_to_data_frame(result_set)
            for player_id, player_name in zip(
                player_stats_df["PLAYER_ID"].tolist(),
                player_stats_df["PLAYER_NAME"].tolist(),
                strict=True,
            ):
                if player_id is None or not player_name:
                    continue
                player_names[int(player_id)] = str(player_name)
            break

    return player_names


def fetch_game_record(
    game_id: str,
    team_abbreviation: str,
    source_data_dir: Path,
) -> GameRecord:
    """Fetch one NBA game and normalize the selected team into a GameRecord."""

    box_score_payload = load_or_fetch_box_score(
        game_id=game_id,
        source_data_dir=source_data_dir,
    )
    player_stats_df = result_set_to_data_frame(box_score_payload["resultSets"][0])
    team_stats_df = result_set_to_data_frame(box_score_payload["resultSets"][1])

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
    players = extract_players_who_appeared(player_rows["PLAYER_ID"], player_rows["MIN"])
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


def extract_players_who_appeared(player_ids, minutes_played) -> set[int]:
    """Return the NBA player ids that logged non-zero minutes in the game."""

    players: set[int] = set()

    for player_id, minutes in zip(player_ids.tolist(), minutes_played.tolist(), strict=True):
        if player_id is None:
            continue
        if not played_in_game(minutes):
            continue
        players.add(int(player_id))

    return players


def played_in_game(minutes: object) -> bool:
    """Return whether the NBA box score minute value indicates game participation."""

    if minutes is None:
        return False

    minute_text = str(minutes).strip()
    if not minute_text:
        return False
    if minute_text in {"0", "0:00", "0.0"}:
        return False

    return True
