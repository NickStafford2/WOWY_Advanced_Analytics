from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Any

from rawr_analytics.metrics._span import build_span_payload
from rawr_analytics.metrics.rawr.records import RawrPlayerSeasonRecord
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrSeriesPointDTO:
    season: str
    value: float | None


@dataclass(frozen=True)
class RawrPlayerSeasonRowDTO:
    season_id: str
    player_id: int
    player_name: str
    value: float
    sample_size: int
    secondary_sample_size: None
    games: int
    average_minutes: float | None
    total_minutes: float | None


@dataclass(frozen=True)
class RawrLeaderboardRowDTO:
    rank: int
    player_id: int
    player_name: str
    span_average_value: float
    average_minutes: float | None
    total_minutes: float | None
    games: int
    season_count: int
    points: list[RawrSeriesPointDTO]


def build_player_seasons_payload(
    rows: Sequence[RawrPlayerSeasonRecord],
) -> dict[str, Any]:
    return {
        "metric": "rawr",
        "rows": [asdict(_build_player_season_row_dto(row)) for row in rows],
    }


def build_leaderboard_payload(
    *,
    metric: str,
    rows: Sequence[RawrPlayerSeasonRecord],
    seasons: list[str],
    top_n: int,
    mode: str,
    available_seasons: list[Season] | None = None,
    available_teams: list[Team] | None = None,
) -> dict[str, Any]:
    table_rows = _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=top_n)
    payload = {
        "mode": mode,
        "metric": metric,
        "span": build_span_payload(seasons=seasons, top_n=top_n),
        "table_rows": [asdict(row) for row in table_rows],
        "series": _build_series_from_table_rows(table_rows),
    }
    if available_seasons is not None:
        payload["available_seasons"] = [season.id for season in available_seasons]
    if available_teams is not None:
        payload["available_teams"] = [team.current.abbreviation for team in available_teams]
    return payload


def build_export_table(
    *,
    rows: Sequence[RawrPlayerSeasonRecord],
    seasons: list[str],
) -> list[dict[str, Any]]:
    return [asdict(row) for row in _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=None)]


def _build_player_season_row_dto(
    row: RawrPlayerSeasonRecord,
) -> RawrPlayerSeasonRowDTO:
    return RawrPlayerSeasonRowDTO(
        season_id=row.season.id,
        player_id=row.player.player_id,
        player_name=row.player.player_name,
        value=row.coefficient,
        sample_size=row.games,
        secondary_sample_size=None,
        games=row.games,
        average_minutes=row.minutes.average_minutes,
        total_minutes=row.minutes.total_minutes,
    )


def _build_ranked_table_rows(
    *,
    rows: Sequence[RawrPlayerSeasonRecord],
    seasons: list[str],
    top_n: int | None,
) -> list[RawrLeaderboardRowDTO]:
    rows_by_player: dict[int, list[RawrPlayerSeasonRecord]] = {}
    for row in rows:
        rows_by_player.setdefault(row.player.player_id, []).append(row)

    ordered_seasons = sorted(dict.fromkeys(seasons))
    full_span_length = len(ordered_seasons) or 1
    ranked_rows: list[RawrLeaderboardRowDTO] = []
    for player_id, player_rows in rows_by_player.items():
        total_minutes_values = [row.minutes.total_minutes for row in player_rows]
        total_minutes = sum(minutes for minutes in total_minutes_values if minutes is not None)
        games = sum(row.games for row in player_rows)
        average_minutes = total_minutes / games if games > 0 else None
        ranked_rows.append(
            RawrLeaderboardRowDTO(
                rank=0,
                player_id=player_id,
                player_name=player_rows[0].player.player_name,
                span_average_value=sum(row.coefficient for row in player_rows) / full_span_length,
                average_minutes=average_minutes,
                total_minutes=total_minutes,
                games=games,
                season_count=len(player_rows),
                points=[
                    RawrSeriesPointDTO(
                        season=season_id,
                        value=next(
                            (row.coefficient for row in player_rows if row.season.id == season_id),
                            None,
                        ),
                    )
                    for season_id in ordered_seasons
                ],
            )
        )

    ranked_rows.sort(
        key=lambda row: (row.span_average_value, row.player_name),
        reverse=True,
    )
    limited_rows = ranked_rows if top_n is None else ranked_rows[:top_n]
    return [
        RawrLeaderboardRowDTO(
            rank=index + 1,
            player_id=row.player_id,
            player_name=row.player_name,
            span_average_value=row.span_average_value,
            average_minutes=row.average_minutes,
            total_minutes=row.total_minutes,
            games=row.games,
            season_count=row.season_count,
            points=row.points,
        )
        for index, row in enumerate(limited_rows)
    ]


def _build_series_from_table_rows(
    table_rows: list[RawrLeaderboardRowDTO],
) -> list[dict[str, Any]]:
    return [
        {
            "player_id": row.player_id,
            "player_name": row.player_name,
            "span_average_value": row.span_average_value,
            "season_count": row.season_count,
            "points": [asdict(point) for point in row.points],
        }
        for row in table_rows
    ]
