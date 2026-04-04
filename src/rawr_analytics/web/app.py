from __future__ import annotations

import json
from typing import Any

from rawr_analytics.metrics import MetricView
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy import WowyQuery, build_wowy_query
from rawr_analytics.services.rawr_query import (
    build_rawr_options_payload,
    build_rawr_query_export,
    build_rawr_query_view,
)
from rawr_analytics.services.rawr_query_params import RawrQuery, build_rawr_query
from rawr_analytics.services.wowy_query import (
    build_wowy_options_payload,
    build_wowy_query_export,
    build_wowy_query_view,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def create_app():
    from flask import Flask, Response, jsonify, request

    app = Flask(__name__)

    def _parse_season_type() -> SeasonType:
        return SeasonType.parse(request.args.get("season_type", "Regular Season"))

    def _parse_rawr_query() -> RawrQuery:
        season_type = _parse_season_type()
        return build_rawr_query(
            season_type=season_type,
            teams=_parse_team_list(request.args.getlist("team_id")),
            seasons=_parse_season_list(request.args.getlist("season"), season_type=season_type),
            top_n=_parse_optional_int(request.args.get("top_n", None)),
            min_average_minutes=_parse_optional_float(
                request.args.get("min_average_minutes", None)
            ),
            min_total_minutes=_parse_optional_float(request.args.get("min_total_minutes", None)),
            min_games=_parse_optional_int(request.args.get("min_games", None)),
            ridge_alpha=_parse_optional_float(request.args.get("ridge_alpha", None)),
        )

    def _parse_wowy_query() -> WowyQuery:
        season_type = _parse_season_type()
        return build_wowy_query(
            season_type=season_type,
            teams=_parse_team_list(request.args.getlist("team_id")),
            seasons=_parse_season_list(request.args.getlist("season"), season_type=season_type),
            top_n=_parse_optional_int(request.args.get("top_n", None)),
            min_average_minutes=_parse_optional_float(
                request.args.get("min_average_minutes", None)
            ),
            min_total_minutes=_parse_optional_float(request.args.get("min_total_minutes", None)),
            min_games_with=_parse_optional_int(request.args.get("min_games_with", None)),
            min_games_without=_parse_optional_int(request.args.get("min_games_without", None)),
        )

    def _parse_rawr_options_query() -> RawrQuery:
        return build_rawr_query(
            season_type=_parse_season_type(),
            teams=_parse_team_list(request.args.getlist("team_id")),
        )

    def _parse_wowy_options_query() -> WowyQuery:
        return build_wowy_query(
            season_type=_parse_season_type(),
            teams=_parse_team_list(request.args.getlist("team_id")),
        )

    def _parse_metric(metric: str) -> Metric:
        return Metric.parse(metric)

    def _json_metric_response(parsed_metric: Metric, view: MetricView):
        if parsed_metric == Metric.RAWR:
            payload = build_rawr_query_view(_parse_rawr_query(), view=view)
        else:
            payload = build_wowy_query_view(parsed_metric, _parse_wowy_query(), view=view)
        return jsonify(payload)

    def run_json(handler):
        try:
            return handler()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    def _csv_metric_response(parsed_metric: Metric, view: MetricView):
        if parsed_metric == Metric.RAWR:
            rows = build_rawr_query_export(_parse_rawr_query(), view=view)
        else:
            rows = build_wowy_query_export(
                parsed_metric,
                _parse_wowy_query(),
                view=view,
            )
        filename = f"{parsed_metric.value}-all-players.csv"
        return Response(
            _render_leaderboard_csv(metric=parsed_metric, table_rows=rows),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.get("/api/metrics/<metric>/options")
    def get_metric_options(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: jsonify(
                build_rawr_options_payload(_parse_rawr_options_query())
                if parsed_metric == Metric.RAWR
                else build_wowy_options_payload(
                    parsed_metric,
                    _parse_wowy_options_query(),
                )
            )
        )

    @app.get("/api/metrics/<metric>/player-seasons")
    def get_metric_player_seasons(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(lambda: _json_metric_response(parsed_metric, "player-seasons"))

    @app.get("/api/metrics/<metric>/span-chart")
    def get_metric_span_chart(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(lambda: _json_metric_response(parsed_metric, "span-chart"))

    @app.get("/api/metrics/<metric>/cached-leaderboard")
    def get_metric_cached_leaderboard(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(lambda: _json_metric_response(parsed_metric, "cached-leaderboard"))

    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    def export_metric_cached_leaderboard(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(lambda: _csv_metric_response(parsed_metric, "cached-leaderboard"))

    @app.get("/api/metrics/<metric>/custom-query")
    def get_metric_custom_query(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(lambda: _json_metric_response(parsed_metric, "custom-query"))

    @app.get("/api/metrics/<metric>/custom-query.csv")
    def export_metric_custom_query(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(lambda: _csv_metric_response(parsed_metric, "custom-query"))

    return app


def _render_leaderboard_csv(
    *,
    metric: Metric,
    table_rows: list[dict[str, Any]],
) -> str:
    import csv
    from io import StringIO

    output = StringIO()
    writer = csv.writer(output)
    column_order = _build_csv_column_order(table_rows)
    writer.writerow([_csv_header_label(column, metric=metric) for column in column_order])
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
        "games",
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


def _csv_header_label(column: str, *, metric: Metric) -> str:
    return {
        "rank": "Rank",
        "player_id": "Player ID",
        "player_name": "Player",
        "span_average_value": _metric_column_label(metric),
        "average_minutes": "Avg Min",
        "total_minutes": "Tot Min",
        "games": "Games",
        "games_with": "With",
        "games_without": "Without",
        "avg_margin_with": "Avg With",
        "avg_margin_without": "Avg Without",
        "season_count": "Seasons",
        "points": "Points",
    }.get(column, column)


def _metric_column_label(metric: Metric) -> str:
    if metric == Metric.RAWR:
        return "RAWR"
    if metric == Metric.WOWY:
        return "WOWY"
    if metric == Metric.WOWY_SHRUNK:
        return "WOWY Shrunk"
    raise ValueError(f"Unknown metric: {metric}")


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
    return [Season.parse(raw_value, season_type.value) for raw_value in raw_values]
