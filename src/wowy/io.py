from __future__ import annotations

import csv
from pathlib import Path

from wowy.types import GameRecord


def load_games_from_csv(csv_path: Path | str) -> list[GameRecord]:
    games: list[GameRecord] = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"game_id", "team", "margin", "players"}
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

            game = GameRecord(
                game_id=row["game_id"],
                team=row["team"],
                margin=margin,
                players=players,
            )
            games.append(game)

    return games
