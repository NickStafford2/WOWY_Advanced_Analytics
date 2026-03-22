from __future__ import annotations

import math
from pathlib import Path
from typing import Callable

import pandas as pd

from wowy.nba.cache import (
    load_cached_payload,
    load_or_fetch_box_score,
    load_or_fetch_box_score_with_source,
)
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type


def result_set_to_data_frame(result_set: dict) -> pd.DataFrame:
    return pd.DataFrame(result_set["rowSet"], columns=result_set["headers"])


def load_player_names_from_cache(source_data_dir: Path) -> dict[int, str]:
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


def fetch_normalized_game_data(
    game_id: str,
    team_abbreviation: str,
    season: str,
    game_date: str,
    opponent: str,
    is_home: bool,
    season_type: str,
    source_data_dir: Path,
    source: str = "nba_api",
    log: Callable[[str], None] | None = print,
) -> tuple[NormalizedGameRecord, list[NormalizedGamePlayerRecord]]:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    box_score_payload = load_or_fetch_box_score(
        game_id=game_id,
        source_data_dir=source_data_dir,
        log=log,
    )
    return normalize_box_score_payload(
        box_score_payload=box_score_payload,
        game_id=game_id,
        team_abbreviation=team_abbreviation,
        season=season,
        game_date=game_date,
        opponent=opponent,
        is_home=is_home,
        season_type=season_type,
        source=source,
    )


def fetch_normalized_game_data_with_source(
    game_id: str,
    team_abbreviation: str,
    season: str,
    game_date: str,
    opponent: str,
    is_home: bool,
    season_type: str,
    source_data_dir: Path,
    source: str = "nba_api",
    log: Callable[[str], None] | None = print,
) -> tuple[NormalizedGameRecord, list[NormalizedGamePlayerRecord], str]:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    box_score_payload, box_score_source = load_or_fetch_box_score_with_source(
        game_id=game_id,
        source_data_dir=source_data_dir,
        log=log,
    )
    normalized_game, normalized_players = normalize_box_score_payload(
        box_score_payload=box_score_payload,
        game_id=game_id,
        team_abbreviation=team_abbreviation,
        season=season,
        game_date=game_date,
        opponent=opponent,
        is_home=is_home,
        season_type=season_type,
        source=source,
    )
    return normalized_game, normalized_players, box_score_source


def normalize_box_score_payload(
    box_score_payload: dict,
    game_id: str,
    team_abbreviation: str,
    season: str,
    game_date: str,
    opponent: str,
    is_home: bool,
    season_type: str,
    source: str = "nba_api",
) -> tuple[NormalizedGameRecord, list[NormalizedGamePlayerRecord]]:
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    player_stats_df = result_set_to_data_frame(box_score_payload["resultSets"][0])
    team_stats_df = result_set_to_data_frame(box_score_payload["resultSets"][1])

    if player_stats_df.empty or team_stats_df.empty:
        raise ValueError(f"Box score is empty for game {game_id!r}")

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
    normalized_players = extract_normalized_game_players(
        game_id=game_id,
        team_abbreviation=team_abbreviation,
        player_rows=player_rows,
    )
    if not any(player.appeared for player in normalized_players):
        raise ValueError(
            f"No active players found for team {team_abbreviation!r} in game {game_id!r}"
        )

    plus_minus = float(team_rows.iloc[0]["PLUS_MINUS"])
    game = NormalizedGameRecord(
        game_id=game_id,
        season=season,
        game_date=game_date,
        team=team_abbreviation,
        opponent=opponent,
        is_home=is_home,
        margin=plus_minus,
        season_type=season_type,
        source=source,
    )
    return game, normalized_players


def extract_players_who_appeared(player_ids, minutes_played) -> set[int]:
    players: set[int] = set()

    for player_id, minutes in zip(player_ids.tolist(), minutes_played.tolist(), strict=True):
        if player_id is None:
            continue
        if not played_in_game(minutes):
            continue
        players.add(int(player_id))

    return players


def extract_normalized_game_players(
    game_id: str,
    team_abbreviation: str,
    player_rows: pd.DataFrame,
) -> list[NormalizedGamePlayerRecord]:
    records: list[NormalizedGamePlayerRecord] = []

    for _, row in player_rows.iterrows():
        player_id = parse_player_id(row["PLAYER_ID"])
        if player_id is None:
            continue

        minutes_raw = row["MIN"]
        minutes = parse_minutes_to_float(minutes_raw)
        records.append(
            NormalizedGamePlayerRecord(
                game_id=game_id,
                team=team_abbreviation,
                player_id=player_id,
                player_name=str(row.get("PLAYER_NAME", "") or player_id),
                appeared=played_in_game(minutes_raw),
                minutes=minutes,
            )
        )

    return records


def parse_player_id(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"Invalid PLAYER_ID value: {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    raise ValueError(f"Invalid PLAYER_ID value type: {type(value).__name__}")


def played_in_game(minutes: object) -> bool:
    parsed_minutes = parse_minutes_to_float(minutes)
    if parsed_minutes is None:
        return False
    return parsed_minutes > 0.0


def parse_minutes_to_float(minutes: object) -> float | None:
    if minutes is None:
        return None
    if isinstance(minutes, bool):
        return None
    if isinstance(minutes, int | float):
        numeric_minutes = float(minutes)
        if not math.isfinite(numeric_minutes):
            return None
        return numeric_minutes

    minute_text = str(minutes).strip()
    if not minute_text:
        return None
    if minute_text in {"0", "0:00", "0.0"}:
        return 0.0
    if ":" not in minute_text:
        try:
            numeric_minutes = float(minute_text)
        except ValueError:
            return None
        if not math.isfinite(numeric_minutes):
            return None
        return numeric_minutes

    whole_minutes, seconds = minute_text.split(":", maxsplit=1)
    try:
        parsed_minutes = float(whole_minutes) + (float(seconds) / 60.0)
    except ValueError:
        return None
    if not math.isfinite(parsed_minutes):
        return None
    return parsed_minutes
