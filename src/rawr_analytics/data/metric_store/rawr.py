from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    build_rawr_player_season_value_row,
)
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def load_rawr_player_season_value_rows(
    *,
    metric_cache_key: str,
    seasons: list[str],
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
) -> list[RawrPlayerSeasonValueRow]:
    assert seasons, "RAWR metric store reads require explicit non-empty seasons"
    initialize_metric_store_db()
    query = """
        SELECT
            rawr.season_id,
            rawr.player_id,
            rawr.player_name,
            rawr.coefficient,
            rawr.games,
            rawr.average_minutes,
            rawr.total_minutes
        FROM rawr_player_season_values AS rawr
        INNER JOIN metric_cache_entry AS cache_entry
            ON cache_entry.metric_cache_entry_id = rawr.metric_cache_entry_id
        WHERE cache_entry.metric_id = 'rawr' AND cache_entry.metric_cache_key = ?
    """
    params: list[object] = [metric_cache_key]
    query += f" AND season_id IN ({','.join('?' for _ in seasons)})"
    params.extend(seasons)
    if min_average_minutes is not None:
        query += " AND COALESCE(average_minutes, 0.0) >= ?"
        params.append(min_average_minutes)
    if min_total_minutes is not None:
        query += " AND COALESCE(total_minutes, 0.0) >= ?"
        params.append(min_total_minutes)
    if min_games is not None:
        query += " AND games >= ?"
        params.append(min_games)
    query += " ORDER BY season_id, coefficient DESC, player_name ASC"
    with connect(METRIC_STORE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [build_rawr_player_season_value_row(row) for row in rows]


def replace_rawr_metric_cache(
    *,
    metric_cache_key: str,
    label: str,
    team_filter: str,
    season_type: SeasonType,
    seasons: list[Season],
    available_teams: list[Team],
    build_version: str,
    source_fingerprint: str,
    rows: list[RawrPlayerSeasonValueRow],
) -> None:
    from datetime import UTC, datetime

    from rawr_analytics.data.metric_store._catalog import (
        build_metric_cache_catalog,
        build_metric_cache_catalog_row,
    )
    from rawr_analytics.data.metric_store._mutations import replace_rawr_metric_cache
    from rawr_analytics.data.metric_store._validation import validate_rawr_rows

    catalog = build_metric_cache_catalog(
        label=label,
        team_filter=team_filter,
        season_type=season_type,
        seasons=seasons,
        available_teams=available_teams,
    )
    updated_at = datetime.now(UTC).isoformat()
    validate_rawr_rows(
        metric_cache_key=metric_cache_key,
        team_filter=team_filter,
        seasons=seasons,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    replace_rawr_metric_cache(
        metric_cache_key=metric_cache_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        updated_at=updated_at,
        catalog_row=build_metric_cache_catalog_row(
            metric_id="rawr",
            metric_cache_key=metric_cache_key,
            catalog=catalog,
            updated_at=updated_at,
        ),
        rows=rows,
        row_count=len(rows),
    )
