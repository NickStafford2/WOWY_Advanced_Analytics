from __future__ import annotations

import csv
from pathlib import Path

from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.team_seasons import TeamSeasonScope


NORMALIZED_GAMES_HEADER = [
    "game_id",
    "season",
    "game_date",
    "team",
    "opponent",
    "is_home",
    "margin",
    "season_type",
    "source",
]

NORMALIZED_GAME_PLAYERS_HEADER = [
    "game_id",
    "team",
    "player_id",
    "player_name",
    "appeared",
    "minutes",
]


def parse_team_season_filename(path: Path) -> TeamSeasonScope:
    parts = path.stem.split("_")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"Unexpected team-season filename {path.name!r}. Expected TEAM_SEASON.csv."
        )
    return TeamSeasonScope(
        team=parts[0].upper(),
        season=canonicalize_season_string(parts[1]),
    )


def load_normalized_games_from_csv(
    csv_path: Path | str,
) -> list[NormalizedGameRecord]:
    games: list[NormalizedGameRecord] = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        missing = set(NORMALIZED_GAMES_HEADER) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for row in reader:
            games.append(
                NormalizedGameRecord(
                    game_id=row["game_id"],
                    season=canonicalize_season_string(row["season"]),
                    game_date=row["game_date"],
                    team=row["team"],
                    opponent=row["opponent"],
                    is_home=_parse_bool(row["is_home"]),
                    margin=float(row["margin"]),
                    season_type=row["season_type"],
                    source=row["source"],
                )
            )

    return games


def load_normalized_game_players_from_csv(
    csv_path: Path | str,
) -> list[NormalizedGamePlayerRecord]:
    players: list[NormalizedGamePlayerRecord] = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        missing = set(NORMALIZED_GAME_PLAYERS_HEADER) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for row in reader:
            minutes_text = (row["minutes"] or "").strip()
            players.append(
                NormalizedGamePlayerRecord(
                    game_id=row["game_id"],
                    team=row["team"],
                    player_id=int(row["player_id"]),
                    player_name=row["player_name"],
                    appeared=_parse_bool(row["appeared"]),
                    minutes=float(minutes_text) if minutes_text else None,
                )
            )

    return players


def _parse_bool(value: str) -> bool:
    text = value.strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")
