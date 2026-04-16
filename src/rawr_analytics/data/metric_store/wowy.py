from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._tables import (
    WowyPlayerSeasonValueRow,
    build_wowy_player_season_value_row,
)
from rawr_analytics.data.metric_store.full_span import MetricStorePlayerSeasonValue
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def load_wowy_player_season_value_rows(
    *,
    metric_id: str,
    scope_key: str,
    seasons: list[str],
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
) -> list[WowyPlayerSeasonValueRow]:
    assert seasons, "WOWY metric store reads require explicit non-empty seasons"
    initialize_metric_store_db()
    query = """
        SELECT
            snapshot.snapshot_id,
            snapshot.metric_id,
            snapshot.scope_key,
            wowy.team_filter,
            wowy.season_type,
            wowy.season_id,
            wowy.player_id,
            wowy.player_name,
            wowy.value,
            wowy.games_with,
            wowy.games_without,
            wowy.avg_margin_with,
            wowy.avg_margin_without,
            wowy.average_minutes,
            wowy.total_minutes,
            wowy.raw_wowy_score
        FROM wowy_player_season_values AS wowy
        INNER JOIN metric_snapshot AS snapshot
            ON snapshot.snapshot_id = wowy.snapshot_id
        WHERE snapshot.metric_id = ? AND snapshot.scope_key = ?
    """
    params: list[object] = [metric_id, scope_key]
    query += f" AND season_id IN ({','.join('?' for _ in seasons)})"
    params.extend(seasons)
    if min_average_minutes is not None:
        query += " AND COALESCE(average_minutes, 0.0) >= ?"
        params.append(min_average_minutes)
    if min_total_minutes is not None:
        query += " AND COALESCE(total_minutes, 0.0) >= ?"
        params.append(min_total_minutes)
    if min_games_with is not None:
        query += " AND games_with >= ?"
        params.append(min_games_with)
    if min_games_without is not None:
        query += " AND games_without >= ?"
        params.append(min_games_without)
    query += " ORDER BY season_id, value DESC, player_name ASC"
    with connect(METRIC_STORE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [build_wowy_player_season_value_row(row) for row in rows]


def replace_wowy_scope_snapshot(
    *,
    metric_id: str,
    scope_key: str,
    label: str,
    team_filter: str,
    season_type: SeasonType,
    seasons: list[Season],
    available_teams: list[Team],
    build_version: str,
    source_fingerprint: str,
    rows: list[WowyPlayerSeasonValueRow],
) -> None:
    from datetime import UTC, datetime

    from rawr_analytics.data.metric_store._catalog import (
        build_metric_scope_catalog,
        build_metric_scope_catalog_row,
    )
    from rawr_analytics.data.metric_store._mutations import replace_wowy_scope_snapshot
    from rawr_analytics.data.metric_store._validation import validate_wowy_rows
    from rawr_analytics.data.metric_store.full_span import build_metric_full_span_rows

    catalog = build_metric_scope_catalog(
        label=label,
        team_filter=team_filter,
        season_type=season_type,
        seasons=seasons,
        available_teams=available_teams,
    )
    updated_at = datetime.now(UTC).isoformat()
    validate_wowy_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        seasons=seasons,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    series_rows, point_rows = build_metric_full_span_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        season_ids=catalog.availability.season_ids,
        player_season_values=_build_player_season_values(rows),
    )
    replace_wowy_scope_snapshot(
        metric_id=metric_id,
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        updated_at=updated_at,
        catalog_row=build_metric_scope_catalog_row(
            metric_id=metric_id,
            scope_key=scope_key,
            catalog=catalog,
            updated_at=updated_at,
        ),
        series_rows=series_rows,
        point_rows=point_rows,
        rows=rows,
        row_count=len(rows),
    )


def _build_player_season_values(
    rows: list[WowyPlayerSeasonValueRow],
) -> list[MetricStorePlayerSeasonValue]:
    player_season_values: list[MetricStorePlayerSeasonValue] = []
    for row in rows:
        if row.value is None:
            continue
        player_season_values.append(
            MetricStorePlayerSeasonValue(
                player_id=row.player_id,
                player_name=row.player_name,
                season_id=row.season_id,
                value=row.value,
            )
        )
    return player_season_values
