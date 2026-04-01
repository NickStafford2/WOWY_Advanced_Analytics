from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.metric_query import (
    MetricOptionsPayload,
    RawrMetricFilters,
    TeamOption,
    WowyMetricFilters,
)
from rawr_analytics.services import (
    MetricQueryRequest,
    build_metric_options_payload,
    build_metric_query_export,
    build_metric_query_view,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def create_app():
    from flask import Flask, Response, jsonify, request

    app = Flask(__name__)

    def parse_metric_query(metric: str) -> MetricQueryRequest:
        metric_type = Metric.parse(metric)
        season_type = SeasonType.parse(request.args.get("season_type", "Regular Season"))
        return MetricQueryRequest(
            metric=metric_type,
            season_type=season_type,
            teams=_parse_team_list(request.args.getlist("team_id")),
            seasons=_parse_season_list(request.args.getlist("season"), season_type=season_type),
            top_n=_parse_optional_int(request.args.get("top_n")),
            min_average_minutes=_parse_optional_float(request.args.get("min_average_minutes")),
            min_total_minutes=_parse_optional_float(request.args.get("min_total_minutes")),
            min_games=_parse_optional_int(request.args.get("min_games")),
            ridge_alpha=_parse_optional_float(request.args.get("ridge_alpha")),
            min_games_with=_parse_optional_int(request.args.get("min_games_with")),
            min_games_without=_parse_optional_int(request.args.get("min_games_without")),
        )

    def json_metric_response(metric: str, view: str):
        metric_type = Metric.parse(metric)
        result = build_metric_query_view(
            parse_metric_query(metric),
            view=view,
        )
        assert result.metric == metric_type
        return jsonify(_serialize_json_value(result.payload))

    def run_json(handler):
        try:
            return handler()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    def csv_metric_response(metric: str, view: str):
        metric_type = Metric.parse(metric)
        result = build_metric_query_export(
            parse_metric_query(metric),
            view=view,
        )
        assert result.metric == metric_type
        filename = f"{metric}-all-players.csv"
        return Response(
            _render_leaderboard_csv(metric_label=result.metric_label, table_rows=result.rows),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.get("/api/metrics/<metric>/options")
    def get_metric_options(metric: str):
        return run_json(
            lambda: jsonify(
                _serialize_json_value(
                    build_metric_options_payload(
                        MetricQueryRequest(
                            metric=Metric.parse(metric),
                            teams=_parse_team_list(request.args.getlist("team_id")),
                            season_type=SeasonType.parse(
                                request.args.get("season_type", "Regular Season")
                            ),
                        )
                    )
                )
            )
        )

    @app.get("/api/metrics/<metric>/player-seasons")
    def get_metric_player_seasons(metric: str):
        return run_json(lambda: json_metric_response(metric, "player-seasons"))

    @app.get("/api/metrics/<metric>/span-chart")
    def get_metric_span_chart(metric: str):
        return run_json(lambda: json_metric_response(metric, "span-chart"))

    @app.get("/api/metrics/<metric>/cached-leaderboard")
    def get_metric_cached_leaderboard(metric: str):
        return run_json(lambda: json_metric_response(metric, "cached-leaderboard"))

    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    def export_metric_cached_leaderboard(metric: str):
        return run_json(lambda: csv_metric_response(metric, "cached-leaderboard"))

    @app.get("/api/metrics/<metric>/custom-query")
    def get_metric_custom_query(metric: str):
        return run_json(lambda: json_metric_response(metric, "custom-query"))

    @app.get("/api/metrics/<metric>/custom-query.csv")
    def export_metric_custom_query(metric: str):
        return run_json(lambda: csv_metric_response(metric, "custom-query"))

    @app.get("/api/wowy/player-seasons")
    def get_wowy_player_seasons():
        return get_metric_player_seasons(Metric.WOWY.value)

    @app.get("/api/wowy/options")
    def get_wowy_options():
        return get_metric_options(Metric.WOWY.value)

    @app.get("/api/wowy/span-chart")
    def get_wowy_span_chart():
        return get_metric_span_chart(Metric.WOWY.value)

    @app.get("/api/wowy/cached-leaderboard")
    def get_wowy_cached_leaderboard():
        return get_metric_cached_leaderboard(Metric.WOWY.value)

    @app.get("/api/wowy/custom-query")
    def get_wowy_custom_query():
        return get_metric_custom_query(Metric.WOWY.value)

    @app.get("/api/wowy-shrunk/custom-query")
    def get_wowy_shrunk_custom_query():
        return get_metric_custom_query(Metric.WOWY_SHRUNK.value)

    @app.get("/api/rawr/custom-query")
    def get_rawr_custom_query():
        return get_metric_custom_query(Metric.RAWR.value)

    return app


def _render_leaderboard_csv(
    *,
    metric_label: str,
    table_rows: list[dict[str, Any]],
) -> str:
    import csv
    from io import StringIO

    output = StringIO()
    writer = csv.writer(output)
    column_order = _build_csv_column_order(table_rows)
    writer.writerow(
        [_csv_header_label(column, metric_label=metric_label) for column in column_order]
    )
    for row in table_rows:
        writer.writerow([_format_csv_value(row.get(column)) for column in column_order])
    return output.getvalue()


def _build_csv_column_order(table_rows: list[dict[str, Any]]) -> list[str]:
    preferred_order = [
        "rank",
        "player_id",
        "player_name",
        "span_average_value",
        "average_minutes",
        "total_minutes",
        "games_with",
        "games_without",
        "avg_margin_with",
        "avg_margin_without",
        "season_count",
        "points",
    ]
    available_columns = {key for row in table_rows for key in row}
    ordered_columns = [column for column in preferred_order if column in available_columns]
    ordered_columns.extend(sorted(available_columns - set(ordered_columns)))
    return ordered_columns


def _csv_header_label(column: str, *, metric_label: str) -> str:
    return {
        "rank": "Rank",
        "player_id": "Player ID",
        "player_name": "Player",
        "span_average_value": metric_label,
        "average_minutes": "Avg Min",
        "total_minutes": "Tot Min",
        "games_with": "With",
        "games_without": "Without",
        "avg_margin_with": "Avg With",
        "avg_margin_without": "Avg Without",
        "season_count": "Seasons",
        "points": "Points",
    }.get(column, column)


def _format_csv_value(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, separators=(",", ":"))
    if isinstance(value, float):
        return f"{value:.6g}"
    if value is None:
        return "—"
    return str(value)


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


def _serialize_json_value(value: Any) -> Any:
    if isinstance(value, Team):
        return value.current.abbreviation
    if isinstance(value, Season):
        return value.id
    if isinstance(value, SeasonType):
        return value.to_nba_format()
    if isinstance(value, Metric):
        return value.value
    if isinstance(value, RawrMetricFilters):
        return _serialize_rawr_metric_filters(value)
    if isinstance(value, WowyMetricFilters):
        return _serialize_wowy_metric_filters(value)
    if isinstance(value, TeamOption):
        return {
            "team_id": value.team.team_id,
            "label": value.label,
            "available_seasons": [season.id for season in value.available_seasons],
        }
    if isinstance(value, MetricOptionsPayload):
        return {
            "metric": value.metric,
            "metric_label": value.metric_label,
            "available_teams": [team.current.abbreviation for team in value.available_teams],
            "team_options": [_serialize_json_value(option) for option in value.team_options],
            "available_seasons": [season.id for season in value.available_seasons],
            "available_teams_by_season": {
                season_id: [team.current.abbreviation for team in teams]
                for season_id, teams in value.available_teams_by_season.items()
            },
            "filters": _serialize_json_value(value.filters),
        }
    if is_dataclass(value):
        return {
            field.name: _serialize_json_value(getattr(value, field.name)) for field in fields(value)
        }
    if isinstance(value, dict):
        return {key: _serialize_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_serialize_json_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    return value


def _serialize_rawr_metric_filters(filters: RawrMetricFilters) -> dict[str, Any]:
    return {
        "team": None if filters.teams is None else [team.current.abbreviation for team in filters.teams],
        "team_id": None if filters.teams is None else [team.team_id for team in filters.teams],
        "season": None if filters.seasons is None else [season.id for season in filters.seasons],
        "season_type": filters.season_type.to_nba_format(),
        "min_average_minutes": filters.min_average_minutes,
        "min_total_minutes": filters.min_total_minutes,
        "top_n": filters.top_n,
        "min_games": filters.min_games,
        "ridge_alpha": filters.ridge_alpha,
    }


def _serialize_wowy_metric_filters(filters: WowyMetricFilters) -> dict[str, Any]:
    return {
        "team": None if filters.teams is None else [team.current.abbreviation for team in filters.teams],
        "team_id": None if filters.teams is None else [team.team_id for team in filters.teams],
        "season": None if filters.seasons is None else [season.id for season in filters.seasons],
        "season_type": filters.season_type.to_nba_format(),
        "min_average_minutes": filters.min_average_minutes,
        "min_total_minutes": filters.min_total_minutes,
        "top_n": filters.top_n,
        "min_games_with": filters.min_games_with,
        "min_games_without": filters.min_games_without,
    }
