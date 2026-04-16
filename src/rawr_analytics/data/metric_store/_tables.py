from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import cast


@dataclass(frozen=True)
class RawrPlayerSeasonValueRow:
    snapshot_id: int | None
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
    player_id: int
    player_name: str
    games: int
    coefficient: float
    average_minutes: float | None
    total_minutes: float | None


@dataclass(frozen=True)
class WowyPlayerSeasonValueRow:
    snapshot_id: int | None
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
    player_id: int
    player_name: str
    value: float | None
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    average_minutes: float | None
    total_minutes: float | None
    raw_wowy_score: float | None


def build_rawr_player_season_value_row(row: sqlite3.Row) -> RawrPlayerSeasonValueRow:
    return RawrPlayerSeasonValueRow(
        snapshot_id=cast(int | None, row["snapshot_id"]),
        metric_id=cast(str, row["metric_id"]),
        scope_key=cast(str, row["scope_key"]),
        team_filter=cast(str, row["team_filter"]),
        season_type=cast(str, row["season_type"]),
        season_id=cast(str, row["season_id"]),
        player_id=cast(int, row["player_id"]),
        player_name=cast(str, row["player_name"]),
        games=cast(int, row["games"]),
        coefficient=cast(float, row["coefficient"]),
        average_minutes=cast(float | None, row["average_minutes"]),
        total_minutes=cast(float | None, row["total_minutes"]),
    )


def build_wowy_player_season_value_row(row: sqlite3.Row) -> WowyPlayerSeasonValueRow:
    return WowyPlayerSeasonValueRow(
        snapshot_id=cast(int | None, row["snapshot_id"]),
        metric_id=cast(str, row["metric_id"]),
        scope_key=cast(str, row["scope_key"]),
        team_filter=cast(str, row["team_filter"]),
        season_type=cast(str, row["season_type"]),
        season_id=cast(str, row["season_id"]),
        player_id=cast(int, row["player_id"]),
        player_name=cast(str, row["player_name"]),
        value=cast(float | None, row["value"]),
        games_with=cast(int, row["games_with"]),
        games_without=cast(int, row["games_without"]),
        avg_margin_with=cast(float | None, row["avg_margin_with"]),
        avg_margin_without=cast(float | None, row["avg_margin_without"]),
        average_minutes=cast(float | None, row["average_minutes"]),
        total_minutes=cast(float | None, row["total_minutes"]),
        raw_wowy_score=cast(float | None, row["raw_wowy_score"]),
    )


def metric_values_table(metric_id: str) -> str:
    if metric_id == "rawr":
        return "rawr_player_season_values"
    if metric_id in {"wowy", "wowy_shrunk"}:
        return "wowy_player_season_values"
    raise ValueError(f"Unknown metric table for {metric_id!r}")
