from __future__ import annotations

import csv
from pathlib import Path

from wowy.atomic_io import atomic_text_writer
from wowy.apps.wowy.models import WowyGameRecord, WowyPlayerSeasonRecord


WOWY_PLAYER_SEASON_HEADER = [
    "season",
    "player_id",
    "player_name",
    "games_with",
    "games_without",
    "avg_margin_with",
    "avg_margin_without",
    "wowy_score",
    "average_minutes",
    "total_minutes",
]


def load_games_from_csv(csv_path: Path | str) -> list[WowyGameRecord]:
    """Load derived WOWY rows: one team-perspective game with semicolon-separated player ids."""
    games: list[WowyGameRecord] = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"game_id", "season", "team", "margin", "players"}
        missing = required_columns - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for row_number, row in enumerate(reader, start=2):
            try:
                margin = float(row["margin"])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid margin at row {row_number}: {row['margin']!r}"
                ) from exc

            players: set[int] = set()
            for player in row["players"].split(";"):
                player_text = player.strip()
                if not player_text:
                    continue
                try:
                    players.add(int(player_text))
                except ValueError as exc:
                    raise ValueError(
                        f"Invalid player id at row {row_number}: {player_text!r}"
                    ) from exc
            if not players:
                raise ValueError(f"Row {row_number} has no players listed")

            game = WowyGameRecord(
                game_id=row["game_id"],
                season=row["season"],
                team=row["team"],
                margin=margin,
                players=players,
            )
            games.append(game)

    return games


def write_player_season_records_csv(
    csv_path: Path | str,
    records: list[WowyPlayerSeasonRecord],
) -> None:
    csv_path = Path(csv_path)
    with atomic_text_writer(csv_path, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=WOWY_PLAYER_SEASON_HEADER)
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "season": record.season,
                    "player_id": record.player_id,
                    "player_name": record.player_name,
                    "games_with": record.games_with,
                    "games_without": record.games_without,
                    "avg_margin_with": record.avg_margin_with,
                    "avg_margin_without": record.avg_margin_without,
                    "wowy_score": record.wowy_score,
                    "average_minutes": "" if record.average_minutes is None else record.average_minutes,
                    "total_minutes": "" if record.total_minutes is None else record.total_minutes,
                }
            )
