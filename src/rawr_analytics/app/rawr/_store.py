from __future__ import annotations

from rawr_analytics.data.metric_store import RawrPlayerSeasonValueRow
from rawr_analytics.metrics.rawr import RawrPlayerSeasonRecord
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season, SeasonType


def build_rawr_record_from_store_row(
    row: RawrPlayerSeasonValueRow,
    *,
    season_type: SeasonType,
) -> RawrPlayerSeasonRecord:
    return RawrPlayerSeasonRecord(
        season=Season.parse(row.season_id, season_type.value),
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
    season_type: SeasonType,
) -> RawrPlayerSeasonValueRow:
    return RawrPlayerSeasonValueRow(
        snapshot_id=None,
        metric_id="rawr",
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type.value,
        season_id=record.season.id,
        player_id=record.player.player_id,
        player_name=record.player.player_name,
        games=record.games,
        coefficient=record.coefficient,
        average_minutes=record.minutes.average_minutes,
        total_minutes=record.minutes.total_minutes,
    )

