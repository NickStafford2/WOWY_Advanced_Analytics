from __future__ import annotations

from flask import Request

from rawr_analytics.metrics.rawr.query.request import RawrQuery, build_rawr_query
from rawr_analytics.metrics.wowy.query.request import WowyQuery, build_wowy_query
from rawr_analytics.shared.season import Season, SeasonType, build_all_nba_history_seasons
from rawr_analytics.shared.team import Team


def build_rawr_query_from_request(request: Request) -> RawrQuery:
    season_type = _parse_season_type(request)
    return build_rawr_query(
        teams=_parse_team_id_list(request.args.getlist("team_id")),
        seasons=_parse_query_seasons(request, season_type=season_type),
        top_n=_parse_optional_int(request.args.get("top_n", None)),
        min_average_minutes=_parse_optional_float(request.args.get("min_average_minutes", None)),
        min_total_minutes=_parse_optional_float(request.args.get("min_total_minutes", None)),
        min_games=_parse_optional_int(request.args.get("min_games", None)),
        ridge_alpha=_parse_optional_float(request.args.get("ridge_alpha", None)),
    )


def build_rawr_options_query_from_request(request: Request) -> RawrQuery:
    season_type = _parse_season_type(request)
    return build_rawr_query(
        seasons=_parse_query_seasons(request, season_type=season_type),
        teams=_parse_team_id_list(request.args.getlist("team_id")),
    )


def build_wowy_query_from_request(request: Request) -> WowyQuery:
    season_type = _parse_season_type(request)
    return build_wowy_query(
        teams=_parse_team_id_list(request.args.getlist("team_id")),
        seasons=_parse_query_seasons(request, season_type=season_type),
        top_n=_parse_optional_int(request.args.get("top_n", None)),
        min_average_minutes=_parse_optional_float(request.args.get("min_average_minutes", None)),
        min_total_minutes=_parse_optional_float(request.args.get("min_total_minutes", None)),
        min_games_with=_parse_optional_int(request.args.get("min_games_with", None)),
        min_games_without=_parse_optional_int(request.args.get("min_games_without", None)),
    )


def build_wowy_options_query_from_request(request: Request) -> WowyQuery:
    season_type = _parse_season_type(request)
    return build_wowy_query(
        seasons=_parse_query_seasons(request, season_type=season_type),
        teams=_parse_team_id_list(request.args.getlist("team_id")),
    )


def _parse_season_type(request: Request) -> SeasonType:
    return SeasonType.parse(request.args.get("season_type", "Regular Season"))


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


def _parse_team_id_list(raw_values: list[str]) -> list[Team] | None:
    team_ids = _parse_positive_int_list(raw_values)
    if team_ids is None:
        return None
    return [Team.from_id(team_id) for team_id in team_ids]


def _parse_query_seasons(
    request: Request,
    *,
    season_type: SeasonType,
) -> list[Season] | None:
    seasons = _parse_season_list(request.args.getlist("season"))
    if seasons is not None:
        return seasons
    if "season_type" not in request.args:
        return None
    return build_all_nba_history_seasons(season_types=[season_type])


def _parse_season_list(raw_values: list[str]) -> list[Season] | None:
    if not raw_values:
        return None
    return [Season.parse_id(raw_value) for raw_value in raw_values]
