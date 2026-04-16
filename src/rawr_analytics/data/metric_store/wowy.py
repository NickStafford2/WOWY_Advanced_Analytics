from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._tables import (
    WowyPlayerSeasonValueRow,
    build_wowy_player_season_value_row,
)
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db
from rawr_analytics.shared.season import Season


def load_wowy_player_season_value_rows(
    *,
    metric_id: str,
    metric_cache_key: str,
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
        INNER JOIN metric_cache_entry AS cache_entry
            ON cache_entry.metric_cache_entry_id = wowy.metric_cache_entry_id
        WHERE cache_entry.metric_id = ? AND cache_entry.metric_cache_key = ?
    """
    params: list[object] = [metric_id, metric_cache_key]
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


def replace_wowy_metric_cache(
    *,
    metric_id: str,
    metric_cache_key: str,
    seasons: list[Season],
    build_version: str,
    source_fingerprint: str,
    rows: list[WowyPlayerSeasonValueRow],
) -> None:
    from datetime import UTC, datetime

    from rawr_analytics.data.metric_store._mutations import replace_wowy_metric_cache
    from rawr_analytics.data.metric_store._validation import validate_wowy_rows

    updated_at = datetime.now(UTC).isoformat()
    validate_wowy_rows(
        metric_id=metric_id,
        metric_cache_key=metric_cache_key,
        seasons=seasons,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    replace_wowy_metric_cache(
        metric_id=metric_id,
        metric_cache_key=metric_cache_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        updated_at=updated_at,
        rows=rows,
        row_count=len(rows),
    )
