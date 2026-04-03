from __future__ import annotations

from collections.abc import Callable

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.services.metric_query import MetricQueryRequest
from rawr_analytics.services.metric_refresh import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    MetricStoreRefreshRequest,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

type GetArgFn = Callable[[str, str | None], str | None]
type GetListFn = Callable[[str], list[str]]


def build_metric_query_request(
    *,
    metric: str,
    get_arg: GetArgFn,
    get_list: GetListFn,
) -> MetricQueryRequest:
    parsed_season_type = SeasonType.parse(get_arg("season_type", "Regular Season"))
    return MetricQueryRequest(
        metric=Metric.parse(metric),
        season_type=parsed_season_type,
        teams=_parse_team_list(get_list("team_id")),
        seasons=_parse_season_list(get_list("season"), season_type=parsed_season_type),
        top_n=_parse_optional_int(get_arg("top_n", None)),
        min_average_minutes=_parse_optional_float(get_arg("min_average_minutes", None)),
        min_total_minutes=_parse_optional_float(get_arg("min_total_minutes", None)),
        min_games=_parse_optional_int(get_arg("min_games", None)),
        ridge_alpha=_parse_optional_float(get_arg("ridge_alpha", None)),
        min_games_with=_parse_optional_int(get_arg("min_games_with", None)),
        min_games_without=_parse_optional_int(get_arg("min_games_without", None)),
    )


def build_metric_options_request(
    *,
    metric: str,
    get_arg: GetArgFn,
    get_list: GetListFn,
) -> MetricQueryRequest:
    parsed_season_type = SeasonType.parse(get_arg("season_type", "Regular Season"))
    return MetricQueryRequest(
        metric=Metric.parse(metric),
        season_type=parsed_season_type,
        teams=_parse_team_list(get_list("team_id")),
    )


def build_metric_store_refresh_request(
    *,
    metric: str,
    season_type: str,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    include_team_scopes: bool = True,
) -> MetricStoreRefreshRequest:
    return MetricStoreRefreshRequest(
        metric=Metric.parse(metric),
        season_type=SeasonType.parse(season_type),
        rawr_ridge_alpha=rawr_ridge_alpha,
        include_team_scopes=include_team_scopes,
    )


def _parse_optional_int(raw_value: str | None) -> int | None:
    return None if raw_value is None else int(raw_value)


def _parse_optional_float(raw_value: str | None) -> float | None:
    return None if raw_value is None else float(raw_value)


def _parse_positive_int_list(raw_values: list[str]) -> list[int] | None:
    if not raw_values:
        return None
    parsed_values: list[int] = []
    for raw_value in raw_values:
        value = int(raw_value)
        if value <= 0:
            raise ValueError("team_id values must be positive integers")
        parsed_values.append(value)
    return parsed_values


def _parse_team_list(raw_values: list[str]) -> list[Team] | None:
    team_ids = _parse_positive_int_list(raw_values)
    if team_ids is None:
        return None
    return [Team.from_id(team_id) for team_id in team_ids]


def _parse_season_list(
    raw_values: list[str],
    *,
    season_type: SeasonType,
) -> list[Season] | None:
    if not raw_values:
        return None
    return [Season(raw_value, season_type.value) for raw_value in raw_values]
