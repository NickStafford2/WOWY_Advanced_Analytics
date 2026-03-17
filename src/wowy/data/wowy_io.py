from __future__ import annotations

import csv
from pathlib import Path

from wowy.atomic_io import atomic_text_writer
from wowy.apps.wowy.models import WowyPlayerSeasonRecord


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
