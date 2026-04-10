from __future__ import annotations

from rawr_analytics.data.metric_store.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.metrics.rawr._cache import RawrSeasonProgressFn, load_rawr_records
from rawr_analytics.metrics.rawr._cache_status import list_incomplete_rawr_season_warnings
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
)
from rawr_analytics.metrics.rawr.inputs import build_rawr_request
from rawr_analytics.metrics.rawr.records import RawrPlayerSeasonRecord, build_player_season_records
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def build_rawr_store_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    seasons: list[Season],
    teams: list[Team] | None,
    ridge_alpha: float,
) -> list[RawrPlayerSeasonValueRow]:
    assert seasons, "RAWR store row builds require explicit non-empty seasons"
    season_games, season_game_players = load_rawr_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
    )
    request = build_rawr_request(
        season_games=season_games,
        season_game_players=season_game_players,
        min_games=1,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    )
    records = build_player_season_records(request)
    return [
        build_rawr_store_row_from_record(
            record,
            scope_key=scope_key,
            team_filter=team_filter,
        )
        for record in records
    ]


def build_rawr_record_from_store_row(
    row: RawrPlayerSeasonValueRow,
) -> RawrPlayerSeasonRecord:
    return RawrPlayerSeasonRecord(
        season=Season.parse(row.season_id, row.season_type),
        player=PlayerSummary(
            player_id=row.player_id,
            player_name=row.player_name,
        ),
        minutes=PlayerMinutes(
            average_minutes=row.average_minutes,
            total_minutes=row.total_minutes,
        ),
        games=row.games,
        coefficient=row.coefficient,
    )


def build_rawr_store_row_from_record(
    record: RawrPlayerSeasonRecord,
    *,
    scope_key: str,
    team_filter: str,
) -> RawrPlayerSeasonValueRow:
    return RawrPlayerSeasonValueRow(
        snapshot_id=None,
        metric_id="rawr",
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=record.season.season_type.value,
        season_id=record.season.year_string_nba_api,
        player_id=record.player.player_id,
        player_name=record.player.player_name,
        games=record.games,
        coefficient=record.coefficient,
        average_minutes=record.minutes.average_minutes,
        total_minutes=record.minutes.total_minutes,
    )


__all__ = [
    "RawrSeasonProgressFn",
    "build_rawr_record_from_store_row",
    "build_rawr_store_row_from_record",
    "build_rawr_store_rows",
    "list_incomplete_rawr_season_warnings",
    "load_rawr_records",
]
