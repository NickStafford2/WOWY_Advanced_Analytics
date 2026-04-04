from __future__ import annotations

import json
from typing import Any

from rawr_analytics.app.rawr.query import RawrQuery, build_rawr_query
from rawr_analytics.app.rawr.service import (
    build_rawr_leaderboard_export,
    build_rawr_leaderboard_payload,
    build_rawr_options_payload,
    build_rawr_player_seasons_payload,
    build_rawr_span_chart_payload,
    resolve_rawr_result,
)
from rawr_analytics.app.wowy.query import WowyQuery, build_wowy_query
from rawr_analytics.app.wowy.service import (
    build_wowy_leaderboard_export,
    build_wowy_leaderboard_payload,
    build_wowy_options_payload,
    build_wowy_player_seasons_payload,
    build_wowy_span_chart_payload,
    resolve_wowy_result,
)
from rawr_analytics.metrics.constants import Metric
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
            recalculate=_parse_optional_bool(request.args.get("recalculate", None)) or False,
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

    def _resolve_rawr_query(
        *,
        rawr_recalculate: bool | None = None,
    ) -> RawrQuery:
        query = _parse_rawr_query()
        if rawr_recalculate is not None:
            return build_rawr_query(
                season_type=query.season_type,
                teams=query.teams,
                seasons=query.seasons,
                top_n=query.top_n,
                min_average_minutes=query.min_average_minutes,
                min_total_minutes=query.min_total_minutes,
                min_games=query.min_games,
                ridge_alpha=query.ridge_alpha,
                recalculate=rawr_recalculate,
            )
        return query

    def run_json(handler):
        try:
            return handler()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    def _json_rawr_leaderboard_response(
        *,
        rawr_recalculate: bool | None = None,
    ):
        query = _resolve_rawr_query(rawr_recalculate=rawr_recalculate)
        result = resolve_rawr_result(query)
        return jsonify(build_rawr_leaderboard_payload(query, result))

    def _json_rawr_player_seasons_response():
        query = _resolve_rawr_query()
        result = resolve_rawr_result(query)
        return jsonify(build_rawr_player_seasons_payload(query, result))

    def _json_rawr_span_chart_response():
        query = _resolve_rawr_query()
        result = resolve_rawr_result(query)
        return jsonify(build_rawr_span_chart_payload(query, result))

    def _json_wowy_leaderboard_response(
        parsed_metric: Metric,
        *,
        recalculate: bool = False,
    ):
        query = _parse_wowy_query()
        result = resolve_wowy_result(query, metric=parsed_metric, recalculate=recalculate)
        return jsonify(build_wowy_leaderboard_payload(query, result))

    def _json_wowy_player_seasons_response(parsed_metric: Metric):
        query = _parse_wowy_query()
        result = resolve_wowy_result(query, metric=parsed_metric)
        return jsonify(build_wowy_player_seasons_payload(query, result))

    def _json_wowy_span_chart_response(parsed_metric: Metric):
        query = _parse_wowy_query()
        result = resolve_wowy_result(query, metric=parsed_metric)
        return jsonify(build_wowy_span_chart_payload(query, result))

    def _csv_rawr_leaderboard_response(
        *,
        rawr_recalculate: bool | None = None,
    ):
        query = _resolve_rawr_query(rawr_recalculate=rawr_recalculate)
        result = resolve_rawr_result(query)
        table_rows = build_rawr_leaderboard_export(result)
        filename = f"{Metric.RAWR.value}-all-players.csv"
        return Response(
            _render_leaderboard_csv(metric=Metric.RAWR, table_rows=table_rows),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    def _csv_wowy_leaderboard_response(
        parsed_metric: Metric,
        *,
        recalculate: bool = False,
    ):
        query = _parse_wowy_query()
        result = resolve_wowy_result(query, metric=parsed_metric, recalculate=recalculate)
        rows = build_wowy_leaderboard_export(query, result)
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
                    _parse_wowy_options_query(),
                    metric=parsed_metric,
                )
            )
        )

    @app.get("/api/metrics/<metric>/player-seasons")
    def get_metric_player_seasons(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: (
                _json_rawr_player_seasons_response()
                if parsed_metric == Metric.RAWR
                else _json_wowy_player_seasons_response(parsed_metric)
            )
        )

    @app.get("/api/metrics/<metric>/span-chart")
    def get_metric_span_chart(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: (
                _json_rawr_span_chart_response()
                if parsed_metric == Metric.RAWR
                else _json_wowy_span_chart_response(parsed_metric)
            )
        )

    @app.get("/api/metrics/<metric>/cached-leaderboard")
    def get_metric_cached_leaderboard(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: (
                _json_rawr_leaderboard_response(rawr_recalculate=False)
                if parsed_metric == Metric.RAWR
                else _json_wowy_leaderboard_response(parsed_metric, recalculate=False)
            )
        )

    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    def export_metric_cached_leaderboard(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: (
                _csv_rawr_leaderboard_response(rawr_recalculate=False)
                if parsed_metric == Metric.RAWR
                else _csv_wowy_leaderboard_response(parsed_metric, recalculate=False)
            )
        )

    @app.get("/api/metrics/<metric>/custom-query")
    def get_metric_custom_query(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: (
                _json_rawr_leaderboard_response(rawr_recalculate=True)
                if parsed_metric == Metric.RAWR
                else _json_wowy_leaderboard_response(parsed_metric, recalculate=True)
            )
        )

    @app.get("/api/metrics/<metric>/custom-query.csv")
    def export_metric_custom_query(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: (
                _csv_rawr_leaderboard_response(rawr_recalculate=True)
                if parsed_metric == Metric.RAWR
                else _csv_wowy_leaderboard_response(parsed_metric, recalculate=True)
            )
        )

    @app.get("/api/metrics/<metric>/leaderboard")
    def get_metric_leaderboard(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: (
                _json_rawr_leaderboard_response()
                if parsed_metric == Metric.RAWR
                else _json_wowy_leaderboard_response(parsed_metric, recalculate=False)
            )
        )

    @app.get("/api/metrics/<metric>/leaderboard.csv")
    def export_metric_leaderboard(metric: str):
        parsed_metric = _parse_metric(metric)
        return run_json(
            lambda: (
                _csv_rawr_leaderboard_response()
                if parsed_metric == Metric.RAWR
                else _csv_wowy_leaderboard_response(parsed_metric, recalculate=False)
            )
        )

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


def _parse_optional_bool(raw_value: str | None) -> bool | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {raw_value!r}")


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
