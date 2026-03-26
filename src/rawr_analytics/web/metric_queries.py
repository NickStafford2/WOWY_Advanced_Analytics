from __future__ import annotations

from pathlib import Path
from typing import Any

from rawr_analytics.data.game_cache import list_cached_team_seasons
from rawr_analytics.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from rawr_analytics.data.game_cache.repository import list_cache_load_rows
from rawr_analytics.data.player_metrics_db.constants import DEFAULT_PLAYER_METRICS_DB_PATH
from rawr_analytics.data.player_metrics_db.models import (
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
)
from rawr_analytics.data.player_metrics_db.queries import (
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_rows,
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
)
from rawr_analytics.metrics.rawr import (
    build_custom_query_rows as build_rawr_custom_query_rows,
)
from rawr_analytics.metrics.rawr import default_filters as rawr_default_filters
from rawr_analytics.metrics.wowy import (
    build_custom_query_rows as build_wowy_custom_query_rows,
)
from rawr_analytics.metrics.wowy import default_filters as wowy_default_filters
from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.nba.team_history import official_continuity_label_for_team_id
from rawr_analytics.web.metric_store import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    RAWR_METRIC,
    WOWY_METRIC,
    WOWY_SHRUNK_METRIC,
    build_scope_key,
)


def build_metric_player_seasons_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_sample_size: int | None,
    min_secondary_sample_size: int | None,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        db_path,
        metric=metric,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_sample_size=min_sample_size,
        min_secondary_sample_size=min_secondary_sample_size,
    )
    return {
        "metric": metric,
        "metric_label": catalog_row.metric_label,
        "rows": [_serialize_metric_player_season_row(row) for row in rows],
    }


def build_cached_metric_leaderboard_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    top_n: int,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_sample_size: int | None,
    min_secondary_sample_size: int | None,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        db_path,
        metric=metric,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_sample_size=min_sample_size,
        min_secondary_sample_size=min_secondary_sample_size,
    )
    leaderboard = _build_leaderboard_payload_from_rows(
        metric=metric,
        metric_label=catalog_row.metric_label,
        rows=rows,
        seasons=seasons or catalog_row.available_seasons,
        top_n=top_n,
        mode="cached",
    )
    leaderboard["available_seasons"] = catalog_row.available_seasons
    leaderboard["available_teams"] = catalog_row.available_teams
    return leaderboard


def build_cached_metric_export_table_rows(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_sample_size: int | None,
    min_secondary_sample_size: int | None,
) -> tuple[str, list[dict[str, Any]]]:
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        db_path,
        metric=metric,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_sample_size=min_sample_size,
        min_secondary_sample_size=min_secondary_sample_size,
    )
    table_rows = _build_ranked_table_rows(
        [_serialize_metric_player_season_row(row) for row in rows],
        seasons=seasons or catalog_row.available_seasons,
        top_n=None,
    )
    return catalog_row.metric_label, table_rows


def build_custom_wowy_leaderboard_payload(
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    top_n: int,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    rows = build_wowy_custom_query_rows(
        WOWY_METRIC,
        teams=teams,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    seasons_in_scope = sorted({row["season"] for row in rows})
    return _build_leaderboard_payload_from_custom_rows(
        metric=WOWY_METRIC,
        metric_label="WOWY",
        rows=rows,
        seasons=seasons_in_scope,
        top_n=top_n,
        mode="custom",
    )


def build_custom_wowy_shrunk_leaderboard_payload(
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    top_n: int,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    rows = build_wowy_custom_query_rows(
        WOWY_SHRUNK_METRIC,
        teams=teams,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    seasons_in_scope = sorted({row["season"] for row in rows})
    table_rows = _build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=top_n)
    return {
        "mode": "custom",
        "metric": WOWY_SHRUNK_METRIC,
        "metric_label": "WOWY Shrunk",
        "span": _build_span_payload(seasons_in_scope, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows, seasons=seasons_in_scope),
    }


def build_custom_rawr_leaderboard_payload(
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    top_n: int,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games: int,
    ridge_alpha: float,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    rows = build_rawr_custom_query_rows(
        teams=teams,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    seasons_in_scope = sorted({row["season"] for row in rows})
    return _build_leaderboard_payload_from_custom_rows(
        metric=RAWR_METRIC,
        metric_label="RAWR",
        rows=rows,
        seasons=seasons_in_scope,
        top_n=top_n,
        mode="custom",
    )


def build_custom_metric_leaderboard_payload(
    metric: str,
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    top_n: int,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
    min_games: int | None = None,
    ridge_alpha: float | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> dict[str, Any]:
    if metric == WOWY_METRIC:
        if min_games_without is None:
            raise ValueError("WOWY custom query requires min_games_without")
        return build_custom_wowy_leaderboard_payload(
            teams=teams,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            top_n=top_n,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(min_games_with or 0),
            min_games_without=min_games_without,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )

    if metric == WOWY_SHRUNK_METRIC:
        if min_games_without is None:
            raise ValueError("WOWY shrunk custom query requires min_games_without")
        return build_custom_wowy_shrunk_leaderboard_payload(
            teams=teams,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            top_n=top_n,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(min_games_with or 0),
            min_games_without=min_games_without,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )

    if metric == RAWR_METRIC:
        if ridge_alpha is None:
            raise ValueError("RAWR custom query requires ridge_alpha")
        return build_custom_rawr_leaderboard_payload(
            teams=teams,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            top_n=top_n,
            player_metrics_db_path=player_metrics_db_path,
            min_games=int(min_games or 0),
            ridge_alpha=ridge_alpha,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )

    raise ValueError(f"Unknown metric: {metric}")


def build_custom_metric_export_table_rows(
    metric: str,
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
    min_games: int | None = None,
    ridge_alpha: float | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    if metric == WOWY_METRIC:
        rows = build_wowy_custom_query_rows(
            WOWY_METRIC,
            teams=teams,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(min_games_with or 0),
            min_games_without=int(min_games_without or 0),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        seasons_in_scope = sorted({row["season"] for row in rows})
        return "WOWY", _build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=None)

    if metric == WOWY_SHRUNK_METRIC:
        rows = build_wowy_custom_query_rows(
            WOWY_SHRUNK_METRIC,
            teams=teams,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(min_games_with or 0),
            min_games_without=int(min_games_without or 0),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        seasons_in_scope = sorted({row["season"] for row in rows})
        return (
            "WOWY Shrunk",
            _build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=None),
        )

    if metric == RAWR_METRIC:
        rows = build_rawr_custom_query_rows(
            teams=teams,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            player_metrics_db_path=player_metrics_db_path,
            min_games=int(min_games or 0),
            ridge_alpha=float(ridge_alpha or DEFAULT_RAWR_RIDGE_ALPHA),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        seasons_in_scope = sorted({row["season"] for row in rows})
        return "RAWR", _build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=None)

    raise ValueError(f"Unknown metric: {metric}")


def build_metric_options_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    team_ids: list[int] | None,
    season_type: str,
) -> dict[str, Any]:
    scope_key, _ = build_scope_key(team_ids=team_ids, season_type=season_type)
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    return {
        "metric": catalog_row.metric,
        "metric_label": catalog_row.metric_label,
        "available_teams": catalog_row.available_teams,
        "team_options": _build_team_options(
            db_path=db_path,
            season_type=catalog_row.season_type,
            available_teams=catalog_row.available_teams,
            available_seasons=catalog_row.available_seasons,
        ),
        "available_seasons": catalog_row.available_seasons,
        "available_teams_by_season": _build_available_teams_by_season(
            db_path=db_path,
            season_type=catalog_row.season_type,
            available_teams=catalog_row.available_teams,
            available_seasons=catalog_row.available_seasons,
        ),
        "filters": build_metric_default_filters_payload(
            metric,
            teams=None,
            team_ids=team_ids,
            season_type=catalog_row.season_type,
        ),
    }


def build_metric_default_filters_payload(
    metric: str,
    *,
    teams: list[str] | None,
    team_ids: list[int] | None = None,
    season_type: str,
) -> dict[str, Any]:
    season_type = canonicalize_season_type(season_type)
    payload: dict[str, Any] = {
        "team": teams,
        "team_id": team_ids,
        "season_type": season_type,
    }
    payload.update(_metric_default_filters(metric))
    return payload


def build_metric_filters_payload(
    metric: str,
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    min_sample_size: str | None,
    min_secondary_sample_size: str | None,
    ridge_alpha: str | None,
    min_average_minutes: str | None,
    min_total_minutes: str | None,
    top_n: str | None = None,
) -> dict[str, Any]:
    defaults = build_metric_default_filters_payload(
        metric,
        teams=teams,
        team_ids=team_ids,
        season_type=season_type,
    )
    payload = {
        "team": teams,
        "team_id": team_ids,
        "season": seasons,
        "season_type": canonicalize_season_type(season_type),
        "min_average_minutes": _parse_optional_float(
            min_average_minutes,
            default=float(defaults["min_average_minutes"]),
        ),
        "min_total_minutes": _parse_optional_float(
            min_total_minutes,
            default=float(defaults["min_total_minutes"]),
        ),
        "top_n": _parse_optional_int(top_n, default=int(defaults["top_n"])),
    }
    if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}:
        payload["min_games_with"] = _parse_optional_int(
            min_sample_size,
            default=int(defaults["min_games_with"]),
        )
        payload["min_games_without"] = _parse_optional_int(
            min_secondary_sample_size,
            default=int(defaults["min_games_without"]),
        )
        return payload
    if metric == RAWR_METRIC:
        payload["min_games"] = _parse_optional_int(
            min_sample_size,
            default=int(defaults["min_games"]),
        )
        payload["ridge_alpha"] = _parse_optional_float(
            ridge_alpha,
            default=float(defaults["ridge_alpha"]),
        )
        return payload
    raise ValueError(f"Unknown metric: {metric}")


def _metric_default_filters(metric: str) -> dict[str, int | float]:
    if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}:
        return wowy_default_filters()
    if metric == RAWR_METRIC:
        return rawr_default_filters()
    raise ValueError(f"Unknown metric: {metric}")


def build_metric_span_chart_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    top_n: int,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    series_rows = load_metric_full_span_series_rows(
        db_path,
        metric=metric,
        scope_key=scope_key,
        top_n=top_n,
    )
    player_ids = [row.player_id for row in series_rows]
    season_points = load_metric_full_span_points_map(
        db_path,
        metric=metric,
        scope_key=scope_key,
        player_ids=player_ids,
    )
    return {
        "metric": metric,
        "metric_label": catalog_row.metric_label,
        "span": {
            "start_season": catalog_row.full_span_start_season,
            "end_season": catalog_row.full_span_end_season,
            "available_seasons": catalog_row.available_seasons,
            "top_n": top_n,
        },
        "series": [
            {
                "player_id": row.player_id,
                "player_name": row.player_name,
                "span_average_value": row.span_average_value,
                "season_count": row.season_count,
                "points": [
                    {
                        "season": season,
                        "value": season_points.get(row.player_id, {}).get(season),
                    }
                    for season in catalog_row.available_seasons
                ],
            }
            for row in series_rows
        ],
    }


def _parse_optional_int(raw_value: str | None, default: int) -> int:
    if raw_value is None:
        return default
    return int(raw_value)


def _parse_optional_float(raw_value: str | None, default: float) -> float:
    if raw_value is None:
        return default
    return float(raw_value)


def _serialize_metric_player_season_row(row: PlayerSeasonMetricRow) -> dict[str, Any]:
    payload = {
        "season": row.season,
        "player_id": row.player_id,
        "player_name": row.player_name,
        "value": row.value,
        "sample_size": row.sample_size,
        "secondary_sample_size": row.secondary_sample_size,
        "average_minutes": row.average_minutes,
        "total_minutes": row.total_minutes,
    }
    payload.update(row.details or {})
    return payload


def _build_leaderboard_payload_from_rows(
    *,
    metric: str,
    metric_label: str,
    rows: list[PlayerSeasonMetricRow],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = _build_ranked_table_rows(
        [_serialize_metric_player_season_row(row) for row in rows],
        seasons=seasons,
        top_n=top_n,
    )
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": _build_span_payload(seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows, seasons=seasons),
    }


def _build_leaderboard_payload_from_custom_rows(
    *,
    metric: str,
    metric_label: str,
    rows: list[dict[str, Any]],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = _build_ranked_table_rows(rows, seasons=seasons, top_n=top_n)
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": _build_span_payload(seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows, seasons=seasons),
    }


def _build_span_payload(seasons: list[str], *, top_n: int) -> dict[str, Any]:
    ordered_seasons = sorted(dict.fromkeys(seasons))
    return {
        "start_season": ordered_seasons[0] if ordered_seasons else None,
        "end_season": ordered_seasons[-1] if ordered_seasons else None,
        "available_seasons": ordered_seasons,
        "top_n": top_n,
    }


def _build_ranked_table_rows(
    rows: list[dict[str, Any]],
    *,
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_player.setdefault(row["player_id"], []).append(row)

    full_span_length = len(sorted(dict.fromkeys(seasons))) or 1
    ranked_rows = []
    for player_id, player_rows in rows_by_player.items():
        player_name = player_rows[0]["player_name"]
        games_with = sum(
            (row.get("games_with") or row.get("sample_size") or 0) for row in player_rows
        )
        games_without = sum(
            (row.get("games_without") or row.get("secondary_sample_size") or 0)
            for row in player_rows
        )
        total_minutes = sum((row.get("total_minutes") or 0.0) for row in player_rows)
        average_minutes = total_minutes / games_with if games_with > 0 else None
        ranked_rows.append(
            {
                "rank": 0,
                "player_id": player_id,
                "player_name": player_name,
                "span_average_value": sum(row["value"] for row in player_rows) / full_span_length,
                "average_minutes": average_minutes,
                "total_minutes": total_minutes,
                "games_with": games_with,
                "games_without": games_without,
                "avg_margin_with": _weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_with",
                    weight_keys=("games_with", "sample_size"),
                ),
                "avg_margin_without": _weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_without",
                    weight_keys=("games_without", "secondary_sample_size"),
                ),
                "season_count": len(player_rows),
                "points": [
                    {
                        "season": season,
                        "value": next(
                            (row["value"] for row in player_rows if row["season"] == season),
                            None,
                        ),
                    }
                    for season in sorted(dict.fromkeys(seasons))
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
    *,
    seasons: list[str],
) -> list[dict[str, Any]]:
    season_order = sorted(dict.fromkeys(seasons))
    rows_by_player = {row["player_id"]: row for row in table_rows}
    series = []
    for row in table_rows:
        points = row.get("points")
        if points is None:
            points = [{"season": season, "value": None} for season in season_order]
        series.append(
            {
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "span_average_value": row["span_average_value"],
                "season_count": row["season_count"],
                "points": rows_by_player.get(row["player_id"], {}).get(
                    "points",
                    [{"season": season, "value": None} for season in season_order],
                ),
            }
        )
    return series


def _weighted_average_rows(
    rows: list[dict[str, Any]],
    *,
    value_key: str,
    weight_keys: tuple[str, str],
) -> float | None:
    weighted_total = 0.0
    weight_total = 0
    for row in rows:
        value = row.get(value_key)
        weight = row.get(weight_keys[0]) or row.get(weight_keys[1]) or 0
        if value is None or weight <= 0:
            continue
        weighted_total += value * weight
        weight_total += weight
    if weight_total == 0:
        return None
    return weighted_total / weight_total


def _build_team_options(
    *,
    db_path: Path,
    season_type: str,
    available_teams: list[str],
    available_seasons: list[str],
) -> list[dict[str, Any]]:
    available_team_set = set(available_teams)
    available_season_set = set(available_seasons)
    seasons_by_team_id: dict[int, set[str]] = {}
    for team_season in list_cached_team_seasons(
        player_metrics_db_path=db_path,
        season_type=season_type,
    ):
        if team_season.team not in available_team_set:
            continue
        if team_season.season not in available_season_set:
            continue
        seasons_by_team_id.setdefault(team_season.team_id, set()).add(team_season.season)
    return [
        {
            "team_id": team_id,
            "label": official_continuity_label_for_team_id(team_id),
            "available_seasons": [
                season for season in available_seasons if season in seasons_by_team_id[team_id]
            ],
        }
        for team_id in sorted(seasons_by_team_id, key=official_continuity_label_for_team_id)
    ]


def _build_available_teams_by_season(
    *,
    db_path: Path,
    season_type: str,
    available_teams: list[str],
    available_seasons: list[str],
) -> dict[str, list[str]]:
    available_team_set = set(available_teams)
    available_season_set = set(available_seasons)
    teams_by_season: dict[str, set[str]] = {season: set() for season in available_seasons}
    for team_season in list_cached_team_seasons(
        player_metrics_db_path=db_path,
        season_type=season_type,
    ):
        if team_season.season not in available_season_set:
            continue
        if team_season.team not in available_team_set:
            continue
        teams_by_season.setdefault(team_season.season, set()).add(team_season.team)
    return {
        season: [team for team in available_teams if team in teams_by_season.get(season, set())]
        for season in available_seasons
    }


def _require_current_metric_scope(
    *,
    db_path: Path,
    metric: str,
    scope_key: str,
) -> MetricScopeCatalogRow:
    catalog_row = load_metric_scope_catalog_row(db_path, metric, scope_key)
    if catalog_row is None:
        raise ValueError("Metric store has not been built for the requested scope")

    metadata = load_metric_store_metadata(db_path, metric, scope_key)
    if metadata is None:
        raise ValueError("Metric store metadata is missing for the requested scope")

    cache_load_rows = list_cache_load_rows(db_path, season_type=catalog_row.season_type)
    if not cache_load_rows:
        raise ValueError(
            "Normalized cache is empty for the requested scope season type. "
            "Rebuild ingest before using cached metrics."
        )

    current_fingerprint = build_normalized_cache_fingerprint(
        db_path,
        season_type=catalog_row.season_type,
    )
    if metadata.source_fingerprint != current_fingerprint:
        raise ValueError(
            "Cached metric store is stale relative to normalized cache. "
            "Refresh the web metric store after ingest is rebuilt."
        )
    return catalog_row
