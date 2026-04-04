from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from rawr_analytics.metrics._span import build_span_payload
from rawr_analytics.metrics.rawr.defaults import describe_metric
from rawr_analytics.metrics.rawr.models import RawrCustomQueryResult, RawrPlayerSeasonValue
from rawr_analytics.metrics.rawr.query import RawrQuery
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrQueryFilters:
    teams: list[Team] | None
    seasons: list[Season] | None
    season_type: SeasonType
    min_average_minutes: float
    min_total_minutes: float
    top_n: int
    min_games: int
    ridge_alpha: float

    @classmethod
    def from_query(cls, query: RawrQuery) -> RawrQueryFilters:
        return cls(
            teams=query.teams,
            seasons=query.seasons,
            season_type=query.season_type,
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            top_n=query.top_n,
            min_games=query.min_games,
            ridge_alpha=query.ridge_alpha,
        )

    def for_options(self) -> RawrQueryFilters:
        return RawrQueryFilters(
            teams=self.teams,
            seasons=None,
            season_type=self.season_type,
            min_average_minutes=self.min_average_minutes,
            min_total_minutes=self.min_total_minutes,
            top_n=self.top_n,
            min_games=self.min_games,
            ridge_alpha=self.ridge_alpha,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "team": (
                None
                if self.teams is None
                else [team.current.abbreviation for team in self.teams]
            ),
            "team_id": None if self.teams is None else [team.team_id for team in self.teams],
            "season": None if self.seasons is None else [season.id for season in self.seasons],
            "season_type": self.season_type.to_nba_format(),
            "min_average_minutes": self.min_average_minutes,
            "min_total_minutes": self.min_total_minutes,
            "top_n": self.top_n,
            "min_games": self.min_games,
            "ridge_alpha": self.ridge_alpha,
        }


def build_player_seasons_payload(
    rows: Sequence[RawrPlayerSeasonValue],
) -> dict[str, Any]:
    return {
        "metric": "rawr",
        "metric_label": describe_metric().label,
        "rows": [_serialize_player_season_row(row) for row in rows],
    }


def build_cached_leaderboard_payload(
    *,
    metric_label: str,
    available_seasons: list[Season],
    available_teams: list[Team],
    rows: Sequence[RawrPlayerSeasonValue],
    seasons: list[str],
    top_n: int,
) -> dict[str, Any]:
    payload = _build_leaderboard_payload(
        metric="rawr",
        metric_label=metric_label,
        rows=rows,
        seasons=seasons,
        top_n=top_n,
        mode="cached",
    )
    payload["available_seasons"] = [season.id for season in available_seasons]
    payload["available_teams"] = [team.current.abbreviation for team in available_teams]
    return payload


def build_custom_leaderboard_payload(
    result: RawrCustomQueryResult,
    *,
    top_n: int,
) -> dict[str, Any]:
    return _build_leaderboard_payload(
        metric=result.metric,
        metric_label=result.metric_label,
        rows=result.rows,
        seasons=sorted({row.season_id for row in result.rows}),
        top_n=top_n,
        mode="custom",
    )


def build_export_table(
    *,
    rows: Sequence[RawrPlayerSeasonValue],
    seasons: list[str],
    metric_label: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    return (
        metric_label or describe_metric().label,
        _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=None),
    )


def _serialize_player_season_row(
    row: RawrPlayerSeasonValue,
) -> dict[str, Any]:
    return {
        "season_id": row.season_id,
        "player_id": row.player.player_id,
        "player_name": row.player.player_name,
        "value": row.result.coefficient,
        "sample_size": row.result.games,
        "secondary_sample_size": None,
        "games": row.result.games,
        "average_minutes": row.minutes.average_minutes,
        "total_minutes": row.minutes.total_minutes,
    }


def _build_leaderboard_payload(
    *,
    metric: str,
    metric_label: str,
    rows: Sequence[RawrPlayerSeasonValue],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=top_n)
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": build_span_payload(seasons=seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows),
    }


def _build_ranked_table_rows(
    *,
    rows: Sequence[RawrPlayerSeasonValue],
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[RawrPlayerSeasonValue]] = {}
    for row in rows:
        rows_by_player.setdefault(row.player.player_id, []).append(row)

    ordered_seasons = sorted(dict.fromkeys(seasons))
    full_span_length = len(ordered_seasons) or 1
    ranked_rows: list[dict[str, Any]] = []
    for player_id, player_rows in rows_by_player.items():
        total_minutes = sum((row.minutes.total_minutes or 0.0) for row in player_rows)
        games = sum(row.result.games for row in player_rows)
        average_minutes = total_minutes / games if games > 0 else None
        ranked_rows.append(
            {
                "rank": 0,
                "player_id": player_id,
                "player_name": player_rows[0].player.player_name,
                "span_average_value": sum(row.result.coefficient for row in player_rows)
                / full_span_length,
                "average_minutes": average_minutes,
                "total_minutes": total_minutes,
                "games_with": games,
                "games_without": 0,
                "avg_margin_with": None,
                "avg_margin_without": None,
                "season_count": len(player_rows),
                "points": [
                    {
                        "season": season_id,
                        "value": next(
                            (
                                row.result.coefficient
                                for row in player_rows
                                if row.season_id == season_id
                            ),
                            None,
                        ),
                    }
                    for season_id in ordered_seasons
                ],
            }
        )

    ranked_rows.sort(
        key=lambda row: (row["span_average_value"], row["player_name"]),
        reverse=True,
    )
    limited_rows = ranked_rows if top_n is None else ranked_rows[:top_n]
    return [{**row, "rank": index + 1} for index, row in enumerate(limited_rows)]


def _build_series_from_table_rows(
    table_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "span_average_value": row["span_average_value"],
            "season_count": row["season_count"],
            "points": row["points"],
        }
        for row in table_rows
    ]
