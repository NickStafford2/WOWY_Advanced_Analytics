from __future__ import annotations

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
from rawr_analytics.web.csv import render_leaderboard_csv


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
            render_leaderboard_csv(metric=Metric.RAWR, table_rows=table_rows),
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
            render_leaderboard_csv(metric=parsed_metric, table_rows=rows),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    def _json_metric_options_response(parsed_metric: Metric):
        if parsed_metric == Metric.RAWR:
            return jsonify(build_rawr_options_payload(_parse_rawr_options_query()))
        return jsonify(
            build_wowy_options_payload(
                _parse_wowy_options_query(),
                metric=parsed_metric,
            )
        )

    def _json_metric_player_seasons_response(parsed_metric: Metric):
        if parsed_metric == Metric.RAWR:
            return _json_rawr_player_seasons_response()
        return _json_wowy_player_seasons_response(parsed_metric)

    def _json_metric_span_chart_response(parsed_metric: Metric):
        if parsed_metric == Metric.RAWR:
            return _json_rawr_span_chart_response()
        return _json_wowy_span_chart_response(parsed_metric)

    def _json_metric_leaderboard_response(
        parsed_metric: Metric,
        *,
        recalculate: bool = False,
    ):
        if parsed_metric == Metric.RAWR:
            return _json_rawr_leaderboard_response(rawr_recalculate=recalculate)
        return _json_wowy_leaderboard_response(parsed_metric, recalculate=recalculate)

    def _csv_metric_leaderboard_response(
        parsed_metric: Metric,
        *,
        recalculate: bool = False,
    ):
        if parsed_metric == Metric.RAWR:
            return _csv_rawr_leaderboard_response(rawr_recalculate=recalculate)
        return _csv_wowy_leaderboard_response(parsed_metric, recalculate=recalculate)

    @app.get("/api/metrics/<metric>/options")
    def get_metric_options(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _json_metric_options_response(parsed_metric))

    @app.get("/api/metrics/<metric>/player-seasons")
    def get_metric_player_seasons(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _json_metric_player_seasons_response(parsed_metric))

    @app.get("/api/metrics/<metric>/span-chart")
    def get_metric_span_chart(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _json_metric_span_chart_response(parsed_metric))

    @app.get("/api/metrics/<metric>/cached-leaderboard")
    def get_metric_cached_leaderboard(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _json_metric_leaderboard_response(parsed_metric, recalculate=False))

    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    def export_metric_cached_leaderboard(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _csv_metric_leaderboard_response(parsed_metric, recalculate=False))

    @app.get("/api/metrics/<metric>/custom-query")
    def get_metric_custom_query(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _json_metric_leaderboard_response(parsed_metric, recalculate=True))

    @app.get("/api/metrics/<metric>/custom-query.csv")
    def export_metric_custom_query(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _csv_metric_leaderboard_response(parsed_metric, recalculate=True))

    @app.get("/api/metrics/<metric>/leaderboard")
    def get_metric_leaderboard(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _json_metric_leaderboard_response(parsed_metric, recalculate=False))

    @app.get("/api/metrics/<metric>/leaderboard.csv")
    def export_metric_leaderboard(metric: str):
        parsed_metric = Metric.parse(metric)
        return run_json(lambda: _csv_metric_leaderboard_response(parsed_metric, recalculate=False))

    return app


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
