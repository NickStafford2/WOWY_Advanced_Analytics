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
from wowy.nba.team_identity import resolve_team_id


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

    team_row, opponent_row = resolve_team_and_opponent_rows(
        team_stats_df=team_stats_df,
        team_abbreviation=team_abbreviation,
        game_id=game_id,
    )
    normalized_team_abbreviation = str(team_row["TEAM_ABBREVIATION"]).strip().upper()
    normalized_opponent_abbreviation = (
        str(opponent_row["TEAM_ABBREVIATION"]).strip().upper()
        if opponent_row is not None
        else opponent.upper()
    )
    team_id = parse_team_id(
        team_row.get("TEAM_ID"),
        label="team",
        game_id=game_id,
        team_abbreviation=normalized_team_abbreviation,
    )
    opponent_team_id = parse_team_id(
        opponent_row.get("TEAM_ID") if opponent_row is not None else None,
        label="opponent",
        game_id=game_id,
        team_abbreviation=normalized_opponent_abbreviation,
    )

    player_rows = player_stats_df.loc[
        player_stats_df["TEAM_ABBREVIATION"] == normalized_team_abbreviation,
    ]
    normalized_players = extract_normalized_game_players(
        game_id=game_id,
        team_abbreviation=normalized_team_abbreviation,
        team_id=team_id,
        player_rows=player_rows,
    )
    if not any(player.appeared for player in normalized_players):
        raise ValueError(
            f"No active players found for team {normalized_team_abbreviation!r} in game {game_id!r}"
        )

    plus_minus = resolve_team_margin(
        team_stats_df=team_stats_df,
        team_abbreviation=normalized_team_abbreviation,
        game_id=game_id,
    )
    game = NormalizedGameRecord(
        game_id=game_id,
        season=season,
        game_date=game_date,
        team=normalized_team_abbreviation,
        opponent=normalized_opponent_abbreviation,
        is_home=is_home,
        margin=plus_minus,
        season_type=season_type,
        source=source,
        team_id=team_id,
        opponent_team_id=opponent_team_id,
    )
    return game, normalized_players


def resolve_team_and_opponent_rows(
    *,
    team_stats_df: pd.DataFrame,
    team_abbreviation: str,
    game_id: str,
):
    team_rows = team_stats_df.loc[
        team_stats_df["TEAM_ABBREVIATION"] == team_abbreviation,
    ]
    if team_rows.empty:
        raise ValueError(
            f"Team {team_abbreviation!r} not found in box score for game {game_id!r}"
        )

    opponent_rows = team_stats_df.loc[
        team_stats_df["TEAM_ABBREVIATION"] != team_abbreviation,
    ]
    if len(opponent_rows) > 1:
        raise ValueError(
            f"Could not derive opponent row for game {game_id!r}: "
            f"expected at most one opponent row, found {len(opponent_rows)}"
        )

    return team_rows.iloc[0], (opponent_rows.iloc[0] if len(opponent_rows) == 1 else None)


def resolve_team_margin(
    *,
    team_stats_df: pd.DataFrame,
    team_abbreviation: str,
    game_id: str,
) -> float:
    team_rows = team_stats_df.loc[
        team_stats_df["TEAM_ABBREVIATION"] == team_abbreviation,
    ]
    if team_rows.empty:
        raise ValueError(
            f"Team {team_abbreviation!r} not found in box score for game {game_id!r}"
        )

    plus_minus = parse_box_score_numeric_value(team_rows.iloc[0]["PLUS_MINUS"])
    if plus_minus is not None:
        return plus_minus

    opponent_rows = team_stats_df.loc[
        team_stats_df["TEAM_ABBREVIATION"] != team_abbreviation,
    ]
    if len(opponent_rows) != 1:
        raise ValueError(
            f"Could not derive margin from team stats for game {game_id!r}: "
            f"expected one opponent row, found {len(opponent_rows)}"
        )

    team_points = parse_box_score_numeric_value(team_rows.iloc[0]["PTS"])
    opponent_points = parse_box_score_numeric_value(opponent_rows.iloc[0]["PTS"])
    if team_points is None or opponent_points is None:
        raise ValueError(
            f"Could not derive margin from team stats for game {game_id!r}: "
            "missing team points"
        )

    return team_points - opponent_points


def parse_box_score_numeric_value(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            return None
        return numeric_value

    text = str(value).strip()
    if not text:
        return None
    try:
        numeric_value = float(text)
    except ValueError:
        return None
    if not math.isfinite(numeric_value):
        return None
    return numeric_value


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
    team_id: int | None,
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
                team_id=team_id,
            )
        )

    return records


def parse_team_id(
    value: object,
    *,
    label: str,
    game_id: str,
    team_abbreviation: str,
) -> int:
    if value is None:
        return resolve_team_id(team_abbreviation)
    try:
        team_id = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid {label} TEAM_ID {value!r} in box score for game {game_id!r}"
        ) from exc
    if team_id <= 0:
        raise ValueError(f"Invalid {label} TEAM_ID {value!r} in box score for game {game_id!r}")
    return team_id


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
