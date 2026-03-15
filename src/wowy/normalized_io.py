from __future__ import annotations

import csv
from pathlib import Path

from wowy.types import NormalizedGamePlayerRecord, NormalizedGameRecord


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


def load_normalized_games_from_csv(
    csv_path: Path | str,
) -> list[NormalizedGameRecord]:
    games: list[NormalizedGameRecord] = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        missing = set(NORMALIZED_GAMES_HEADER) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for row_number, row in enumerate(reader, start=2):
            games.append(
                NormalizedGameRecord(
                    game_id=row["game_id"],
                    season=require_text(row["season"], "season", row_number),
                    game_date=require_text(row["game_date"], "game_date", row_number),
                    team=require_text(row["team"], "team", row_number),
                    opponent=require_text(row["opponent"], "opponent", row_number),
                    is_home=parse_bool(row["is_home"], "is_home", row_number),
                    margin=parse_float(row["margin"], "margin", row_number),
                    season_type=require_text(
                        row["season_type"], "season_type", row_number
                    ),
                    source=require_text(row["source"], "source", row_number),
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

        for row_number, row in enumerate(reader, start=2):
            player_id = parse_int(row["player_id"], "player_id", row_number)
            player_name = require_text(row["player_name"], "player_name", row_number)
            appeared = parse_bool(row["appeared"], "appeared", row_number)
            minutes = parse_optional_float(row["minutes"], "minutes", row_number)

            players.append(
                NormalizedGamePlayerRecord(
                    game_id=row["game_id"],
                    team=require_text(row["team"], "team", row_number),
                    player_id=player_id,
                    player_name=player_name,
                    appeared=appeared,
                    minutes=minutes,
                )
            )

    return players


def write_normalized_games_csv(
    csv_path: Path | str,
    games: list[NormalizedGameRecord],
) -> None:
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=NORMALIZED_GAMES_HEADER)
        writer.writeheader()
        for game in games:
            writer.writerow(
                {
                    "game_id": game.game_id,
                    "season": game.season,
                    "game_date": game.game_date,
                    "team": game.team,
                    "opponent": game.opponent,
                    "is_home": format_bool(game.is_home),
                    "margin": game.margin,
                    "season_type": game.season_type,
                    "source": game.source,
                }
            )


def write_normalized_game_players_csv(
    csv_path: Path | str,
    players: list[NormalizedGamePlayerRecord],
) -> None:
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=NORMALIZED_GAME_PLAYERS_HEADER)
        writer.writeheader()
        for player in players:
            writer.writerow(
                {
                    "game_id": player.game_id,
                    "team": player.team,
                    "player_id": player.player_id,
                    "player_name": player.player_name,
                    "appeared": format_bool(player.appeared),
                    "minutes": "" if player.minutes is None else player.minutes,
                }
            )


def require_text(value: str | None, field_name: str, row_number: int) -> str:
    text = (value or "").strip()
    if not text:
        raise ValueError(f"Invalid {field_name} at row {row_number}: {value!r}")
    return text


def parse_bool(value: str | None, field_name: str, row_number: int) -> bool:
    text = (value or "").strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    raise ValueError(f"Invalid {field_name} at row {row_number}: {value!r}")


def format_bool(value: bool) -> str:
    return "true" if value else "false"


def parse_float(value: str | None, field_name: str, row_number: int) -> float:
    try:
        return float(value or "")
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} at row {row_number}: {value!r}") from exc


def parse_optional_float(
    value: str | None,
    field_name: str,
    row_number: int,
) -> float | None:
    text = (value or "").strip()
    if not text:
        return None
    return parse_float(text, field_name, row_number)


def parse_int(value: str | None, field_name: str, row_number: int) -> int:
    try:
        return int(value or "")
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} at row {row_number}: {value!r}") from exc
