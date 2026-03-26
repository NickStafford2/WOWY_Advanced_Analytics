from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.data.game_cache import list_cached_team_seasons
from rawr_analytics.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from rawr_analytics.data.game_cache.repository import list_cache_load_rows
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
    RAWR_METRIC,
)
from rawr_analytics.metrics.rawr import (
    build_custom_query as build_rawr_custom_query,
)
from rawr_analytics.metrics.rawr import (
    default_filters as rawr_default_filters,
)
from rawr_analytics.metrics.rawr import (
    validate_filters as validate_rawr_filters,
)
from rawr_analytics.metrics.scope import build_scope_key
from rawr_analytics.metrics.wowy import (
    WOWY_METRIC,
    WOWY_SHRUNK_METRIC,
)
from rawr_analytics.metrics.wowy import (
    build_custom_query as build_wowy_custom_query,
)
from rawr_analytics.metrics.wowy import (
    default_filters as wowy_default_filters,
)
from rawr_analytics.metrics.wowy import (
    validate_filters as validate_wowy_filters,
)
from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.nba.seasons import canonicalize_season_string
from rawr_analytics.nba.team_history import official_continuity_label_for_team_id

MetricView = str


@dataclass(frozen=True)
class MetricQuery:
    season_type: str
    team_ids: list[int] | None
    seasons: list[str] | None
    top_n: int
    min_average_minutes: float
    min_total_minutes: float
    min_games: int | None = None
    ridge_alpha: float | None = None
    min_games_with: int | None = None
    min_games_without: int | None = None


def build_metric_query(
    metric: str,
    *,
    season_type: str = "Regular Season",
    team_ids: list[int] | None = None,
    seasons: list[str] | None = None,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
    ridge_alpha: float | None = None,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
) -> MetricQuery:
    defaults = _metric_default_filters(metric)
    normalized_team_ids = sorted({team_id for team_id in team_ids or [] if team_id > 0}) or None
    normalized_seasons = (
        [canonicalize_season_string(season) for season in seasons] if seasons else None
    )
    season_type = canonicalize_season_type(season_type)
    top_n = int(top_n if top_n is not None else defaults["top_n"])
    min_average_minutes = float(
        min_average_minutes if min_average_minutes is not None else defaults["min_average_minutes"]
    )
    min_total_minutes = float(
        min_total_minutes if min_total_minutes is not None else defaults["min_total_minutes"]
    )

    if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}:
        min_games_with = int(
            min_games_with if min_games_with is not None else defaults["min_games_with"]
        )
        min_games_without = int(
            min_games_without if min_games_without is not None else defaults["min_games_without"]
        )
        validate_wowy_filters(
            min_games_with,
            min_games_without,
            top_n=top_n,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        return MetricQuery(
            season_type=season_type,
            team_ids=normalized_team_ids,
            seasons=normalized_seasons,
            top_n=top_n,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
            min_games_with=min_games_with,
            min_games_without=min_games_without,
        )

    if metric == RAWR_METRIC:
        min_games = int(min_games if min_games is not None else defaults["min_games"])
        ridge_alpha = float(ridge_alpha if ridge_alpha is not None else defaults["ridge_alpha"])
        validate_rawr_filters(
            min_games,
            ridge_alpha=ridge_alpha,
            top_n=top_n,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        return MetricQuery(
            season_type=season_type,
            team_ids=normalized_team_ids,
            seasons=normalized_seasons,
            top_n=top_n,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
            min_games=min_games,
            ridge_alpha=ridge_alpha,
        )

    raise ValueError(f"Unknown metric: {metric}")


def build_metric_options_payload(
    metric: str,
    *,
    team_ids: list[int] | None,
    season_type: str,
) -> dict[str, Any]:
    query = build_metric_query(metric, team_ids=team_ids, season_type=season_type)
    filters = _build_filters_payload(query)
    filters.pop("season", None)
    scope_key, _ = build_scope_key(team_ids=query.team_ids, season_type=query.season_type)
    catalog_row = _require_current_metric_scope(
        metric=metric,
        scope_key=scope_key,
    )
    return {
        "metric": catalog_row.metric,
        "metric_label": catalog_row.metric_label,
        "available_teams": catalog_row.available_teams,
        "team_options": _build_team_options(
            season_type=catalog_row.season_type,
            available_teams=catalog_row.available_teams,
            available_seasons=catalog_row.available_seasons,
        ),
        "available_seasons": catalog_row.available_seasons,
        "available_teams_by_season": _build_available_teams_by_season(
            season_type=catalog_row.season_type,
            available_teams=catalog_row.available_teams,
            available_seasons=catalog_row.available_seasons,
        ),
        "filters": filters,
    }


def build_metric_view_payload(
    metric: str,
    *,
    view: MetricView,
    query: MetricQuery,
) -> dict[str, Any]:
    scope_key, _ = build_scope_key(team_ids=query.team_ids, season_type=query.season_type)
    if view == "player-seasons":
        payload = _build_metric_player_seasons_payload(
            metric,
            scope_key=scope_key,
            query=query,
        )
    elif view == "span-chart":
        payload = _build_metric_span_chart_payload(
            metric,
            scope_key=scope_key,
            top_n=query.top_n,
        )
    elif view == "cached-leaderboard":
        payload = _build_cached_metric_leaderboard_payload(
            metric,
            scope_key=scope_key,
            query=query,
        )
    elif view == "custom-query":
        payload = _build_custom_metric_leaderboard_payload(
            metric,
            query=query,
        )
    else:
        raise ValueError(f"Unknown metric view: {view}")
    payload["filters"] = _build_filters_payload(query)
    return payload


def build_metric_export_table(
    metric: str,
    *,
    view: MetricView,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    scope_key, _ = build_scope_key(team_ids=query.team_ids, season_type=query.season_type)
    if view == "cached-leaderboard":
        return _build_cached_metric_export_table_rows(
            metric,
            scope_key=scope_key,
            query=query,
        )
    if view == "custom-query":
        return _build_custom_metric_export_table_rows(
            metric,
            query=query,
        )
    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_filters_payload(query: MetricQuery) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "team": None,
        "team_id": query.team_ids,
        "season": query.seasons,
        "season_type": query.season_type,
        "min_average_minutes": query.min_average_minutes,
        "min_total_minutes": query.min_total_minutes,
        "top_n": query.top_n,
    }
    if query.min_games is not None:
        payload["min_games"] = query.min_games
    if query.ridge_alpha is not None:
        payload["ridge_alpha"] = query.ridge_alpha
    if query.min_games_with is not None:
        payload["min_games_with"] = query.min_games_with
    if query.min_games_without is not None:
        payload["min_games_without"] = query.min_games_without
    return payload


def _metric_default_filters(metric: str) -> dict[str, int | float]:
    if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}:
        return wowy_default_filters()
    if metric == RAWR_METRIC:
        return rawr_default_filters()
    raise ValueError(f"Unknown metric: {metric}")


def _build_metric_player_seasons_payload(
    metric: str,
    *,
    scope_key: str,
    query: MetricQuery,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        metric=metric,
        scope_key=scope_key,
        seasons=query.seasons,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_sample_size=_metric_min_sample_size(query),
        min_secondary_sample_size=_metric_secondary_sample_size(query),
    )
    return {
        "metric": metric,
        "metric_label": catalog_row.metric_label,
        "rows": [_serialize_metric_player_season_row(row) for row in rows],
    }


def _build_cached_metric_leaderboard_payload(
    metric: str,
    *,
    scope_key: str,
    query: MetricQuery,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        metric=metric,
        scope_key=scope_key,
        seasons=query.seasons,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_sample_size=_metric_min_sample_size(query),
        min_secondary_sample_size=_metric_secondary_sample_size(query),
    )
    payload = _build_leaderboard_payload_from_rows(
        metric=metric,
        metric_label=catalog_row.metric_label,
        rows=rows,
        seasons=query.seasons or catalog_row.available_seasons,
        top_n=query.top_n,
        mode="cached",
    )
    payload["available_seasons"] = catalog_row.available_seasons
    payload["available_teams"] = catalog_row.available_teams
    return payload


def _build_cached_metric_export_table_rows(
    metric: str,
    *,
    scope_key: str,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    catalog_row = _require_current_metric_scope(
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        metric=metric,
        scope_key=scope_key,
        seasons=query.seasons,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_sample_size=_metric_min_sample_size(query),
        min_secondary_sample_size=_metric_secondary_sample_size(query),
    )
    table_rows = _build_ranked_table_rows(
        [_serialize_metric_player_season_row(row) for row in rows],
        seasons=query.seasons or catalog_row.available_seasons,
        top_n=None,
    )
    return catalog_row.metric_label, table_rows


def _build_custom_metric_leaderboard_payload(
    metric: str,
    *,
    query: MetricQuery,
) -> dict[str, Any]:
    custom_query = _build_custom_metric_query(
        metric,
        query=query,
    )
    rows = custom_query["rows"]
    seasons_in_scope = sorted({row["season"] for row in rows})
    return _build_leaderboard_payload_from_custom_rows(
        metric=custom_query["metric"],
        metric_label=custom_query["metric_label"],
        rows=rows,
        seasons=seasons_in_scope,
        top_n=query.top_n,
        mode="custom",
    )


def _build_custom_metric_export_table_rows(
    metric: str,
    *,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    custom_query = _build_custom_metric_query(
        metric,
        query=query,
    )
    rows = custom_query["rows"]
    seasons_in_scope = sorted({row["season"] for row in rows})
    return (
        custom_query["metric_label"],
        _build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=None),
    )


def _build_custom_metric_query(
    metric: str,
    *,
    query: MetricQuery,
) -> dict[str, Any]:
    if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}:
        return build_wowy_custom_query(
            metric,
            teams=None,
            team_ids=query.team_ids,
            seasons=query.seasons,
            season_type=query.season_type,
            min_games_with=int(query.min_games_with or 0),
            min_games_without=int(query.min_games_without or 0),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
        )
    if metric == RAWR_METRIC:
        return build_rawr_custom_query(
            teams=None,
            team_ids=query.team_ids,
            seasons=query.seasons,
            season_type=query.season_type,
            min_games=int(query.min_games or 0),
            ridge_alpha=float(query.ridge_alpha or rawr_default_filters()["ridge_alpha"]),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
        )
    raise ValueError(f"Unknown metric: {metric}")


def _build_metric_span_chart_payload(
    metric: str,
    *,
    scope_key: str,
    top_n: int,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        metric=metric,
        scope_key=scope_key,
    )
    series_rows = load_metric_full_span_series_rows(
        metric=metric,
        scope_key=scope_key,
        top_n=top_n,
    )
    player_ids = [row.player_id for row in series_rows]
    season_points = load_metric_full_span_points_map(
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


def _metric_min_sample_size(query: MetricQuery) -> int | None:
    return query.min_games_with if query.min_games_with is not None else query.min_games


def _metric_secondary_sample_size(query: MetricQuery) -> int | None:
    return query.min_games_without


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
    season_type: str,  # TODO: Make season type enum
    available_teams: list[str],
    available_seasons: list[str],
) -> list[dict[str, Any]]:
    available_team_set = set(available_teams)
    available_season_set = set(available_seasons)
    seasons_by_team_id: dict[int, set[str]] = {}
    for team_season in list_cached_team_seasons(season_type=season_type):
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
    season_type: str,
    available_teams: list[str],
    available_seasons: list[str],
) -> dict[str, list[str]]:
    available_team_set = set(available_teams)
    available_season_set = set(available_seasons)
    teams_by_season: dict[str, set[str]] = {season: set() for season in available_seasons}
    for team_season in list_cached_team_seasons(season_type=season_type):
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
    metric: str,
    scope_key: str,
) -> MetricScopeCatalogRow:
    catalog_row = load_metric_scope_catalog_row(metric, scope_key)
    if catalog_row is None:
        raise ValueError("Metric store has not been built for the requested scope")

    metadata = load_metric_store_metadata(metric, scope_key)
    if metadata is None:
        raise ValueError("Metric store metadata is missing for the requested scope")

    cache_load_rows = list_cache_load_rows(season_type=catalog_row.season_type)
    if not cache_load_rows:
        raise ValueError(
            "Normalized cache is empty for the requested scope season type. "
            "Rebuild ingest before using cached metrics."
        )

    current_fingerprint = build_normalized_cache_fingerprint(season_type=catalog_row.season_type)
    if metadata.source_fingerprint != current_fingerprint:
        raise ValueError(
            "Cached metric store is stale relative to normalized cache. "
            "Refresh the web metric store after ingest is rebuilt."
        )
    return catalog_row
