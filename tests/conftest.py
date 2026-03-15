from __future__ import annotations

import csv
from pathlib import Path

import pytest


@pytest.fixture
def write_games_csv():
    def _write_games_csv(csv_path: Path, rows: list[list[str]]) -> None:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "season", "team", "margin", "players"])
            writer.writerows(rows)

    return _write_games_csv
