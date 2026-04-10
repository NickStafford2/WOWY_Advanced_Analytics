from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Any, cast

from rawr_analytics.metrics._span import build_span_payload
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.calculate.records import WowyPlayerSeasonValue
from rawr_analytics.metrics.wowy.query.request import WowyQuery
from rawr_analytics.shared.common import JSONDict
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class WowyQueryFiltersDTO:
    team_filter: list[Team] | None
    season_filter: list[Season] | None
    season_type: SeasonType
    min_average_minutes: float
    min_total_minutes: float
    top_n: int
    min_games_with: int
    min_games_without: int

    @classmethod
    def from_query(cls, query: WowyQuery) -> WowyQueryFiltersDTO:
        return cls(
            team_filter=query.teams,
            season_filter=query.seasons,
            season_type=query.season_type,
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            top_n=query.top_n,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )

    def for_options(self) -> WowyQueryFiltersDTO:
        return WowyQueryFiltersDTO(
            team_filter=self.team_filter,
            season_filter=None,
            season_type=self.season_type,
            min_average_minutes=self.min_average_minutes,
            min_total_minutes=self.min_total_minutes,
            top_n=self.top_n,
            min_games_with=self.min_games_with,
            min_games_without=self.min_games_without,
        )

    def to_payload(self) -> JSONDict:
        return {
            "team_filter": None
            if self.team_filter is None
            else [team.current.abbreviation for team in self.team_filter],
            "team_id_filter": None
            if self.team_filter is None
            else [team.team_id for team in self.team_filter],
            "season_filter": None
            if self.season_filter is None
            else [season.year_string_nba_api for season in self.season_filter],
            "season_type": self.season_type.to_nba_format(),
            "min_average_minutes": self.min_average_minutes,
            "min_total_minutes": self.min_total_minutes,
            "top_n": self.top_n,
            "min_games_with": self.min_games_with,
            "min_games_without": self.min_games_without,
        }


@dataclass(frozen=True)
class WowySeriesPointDTO:
    season: str
    value: float | None


@dataclass(frozen=True)
class WowyPlayerSeasonRowDTO:
    season_id: str
    player_id: int
    player_name: str
    value: float | None
    sample_size: int
    secondary_sample_size: int
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    average_minutes: float | None
    total_minutes: float | None
    raw_wowy_score: float | None


@dataclass(frozen=True)
class WowyLeaderboardRowDTO:
    rank: int
    player_id: int
    player_name: str
    span_average_value: float
    average_minutes: float | None
    total_minutes: float
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    season_count: int
    points: list[WowySeriesPointDTO]


def build_wowy_player_seasons_payload(
    *,
    metric: Metric,
    rows: Sequence[WowyPlayerSeasonValue],
) -> JSONDict:
    return {
        "metric": metric.value,
        "rows": [asdict(_build_wowy_player_season_row_dto(row)) for row in rows],
    }


def build_wowy_leaderboard_payload(
    *,
    metric: Metric,
    rows: Sequence[WowyPlayerSeasonValue],
    seasons: list[str],
    top_n: int,
    mode: str,
    available_seasons: list[Season] | None = None,
    available_teams: list[Team] | None = None,
) -> JSONDict:
    table_rows = _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=top_n)
    payload = cast(
        JSONDict,
        {
            "mode": mode,
            "metric": metric.value,
            "span": build_span_payload(seasons=seasons, top_n=top_n),
            "table_rows": [asdict(row) for row in table_rows],
            "series": _build_series_from_table_rows(table_rows),
        },
    )
    if available_seasons is not None:
        payload["available_seasons"] = [season.year_string_nba_api for season in available_seasons]
    if available_teams is not None:
        payload["available_teams"] = [team.current.abbreviation for team in available_teams]
    return payload


def build_wowy_export_rows(
    *,
    rows: Sequence[WowyPlayerSeasonValue],
    seasons: list[str],
) -> list[dict[str, Any]]:
    return [asdict(row) for row in _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=None)]


def build_wowy_span_chart_payload(
    *,
    metric: Metric,
    rows: Sequence[WowyPlayerSeasonValue],
    seasons: list[str],
    top_n: int,
) -> JSONDict:
    table_rows = _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=top_n)
    return cast(
        JSONDict,
        {
            "metric": metric.value,
            "span": {
                "start_season": seasons[0] if seasons else None,
                "end_season": seasons[-1] if seasons else None,
                "available_seasons": seasons,
                "top_n": top_n,
            },
            "series": _build_series_from_table_rows(table_rows),
        },
    )


def _build_wowy_player_season_row_dto(row: WowyPlayerSeasonValue) -> WowyPlayerSeasonRowDTO:
    return WowyPlayerSeasonRowDTO(
        season_id=row.season_id,
        player_id=row.player.player_id,
        player_name=row.player.player_name,
        value=row.result.value,
        sample_size=row.result.games_with,
        secondary_sample_size=row.result.games_without,
        games_with=row.result.games_with,
        games_without=row.result.games_without,
        avg_margin_with=row.result.avg_margin_with,
        avg_margin_without=row.result.avg_margin_without,
        average_minutes=row.minutes.average_minutes,
        total_minutes=row.minutes.total_minutes,
        raw_wowy_score=row.result.raw_value,
    )


def _build_ranked_table_rows(
    *,
    rows: Sequence[WowyPlayerSeasonValue],
    seasons: list[str],
    top_n: int | None,
) -> list[WowyLeaderboardRowDTO]:
    rows_by_player: dict[int, list[WowyPlayerSeasonValue]] = {}
    for row in rows:
        rows_by_player.setdefault(row.player.player_id, []).append(row)

    ordered_seasons = sorted(dict.fromkeys(seasons))
    full_span_length = len(ordered_seasons) or 1
    ranked_rows: list[WowyLeaderboardRowDTO] = []
    for player_id, player_rows in rows_by_player.items():
        total_minutes = sum((row.minutes.total_minutes or 0.0) for row in player_rows)
        games_with = sum(row.result.games_with for row in player_rows)
        games_without = sum(row.result.games_without for row in player_rows)
        average_minutes = total_minutes / games_with if games_with > 0 else None
        ranked_rows.append(
            WowyLeaderboardRowDTO(
                rank=0,
                player_id=player_id,
                player_name=player_rows[0].player.player_name,
                span_average_value=sum(
                    row.result.value for row in player_rows if row.result.value is not None
                )
                / full_span_length,
                average_minutes=average_minutes,
                total_minutes=total_minutes,
                games_with=games_with,
                games_without=games_without,
                avg_margin_with=_weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_with",
                    weight_key="games_with",
                ),
                avg_margin_without=_weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_without",
                    weight_key="games_without",
                ),
                season_count=len(player_rows),
                points=[
                    WowySeriesPointDTO(
                        season=season_id,
                        value=next(
                            (row.result.value for row in player_rows if row.season_id == season_id),
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
        WowyLeaderboardRowDTO(
            rank=index + 1,
            player_id=row.player_id,
            player_name=row.player_name,
            span_average_value=row.span_average_value,
            average_minutes=row.average_minutes,
            total_minutes=row.total_minutes,
            games_with=row.games_with,
            games_without=row.games_without,
            avg_margin_with=row.avg_margin_with,
            avg_margin_without=row.avg_margin_without,
            season_count=row.season_count,
            points=row.points,
        )
        for index, row in enumerate(limited_rows)
    ]


def _build_series_from_table_rows(
    table_rows: list[WowyLeaderboardRowDTO],
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


def _weighted_average_rows(
    rows: Sequence[WowyPlayerSeasonValue],
    *,
    value_key: str,
    weight_key: str,
) -> float | None:
    weighted_total = 0.0
    weight_total = 0
    for row in rows:
        if value_key == "avg_margin_with":
            value = row.result.avg_margin_with
        elif value_key == "avg_margin_without":
            value = row.result.avg_margin_without
        else:
            raise ValueError(f"Unsupported WOWY weighted value key: {value_key}")

        if weight_key == "games_with":
            weight = row.result.games_with
        elif weight_key == "games_without":
            weight = row.result.games_without
        else:
            raise ValueError(f"Unsupported WOWY weighted weight key: {weight_key}")
        if value is None or weight <= 0:
            continue
        weighted_total += value * weight
        weight_total += weight
    if weight_total == 0:
        return None
    return weighted_total / weight_total
