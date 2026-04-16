from __future__ import annotations

from rawr_analytics.data.metric_store.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.metrics.rawr.calculate.records import RawrPlayerSeasonRecord
from rawr_analytics.metrics.rawr.refresh.records import build_rawr_refresh_records
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def build_rawr_metric_store_rows(
    *,
    seasons: list[Season],
    teams: list[Team],
    rawr_ridge_alpha: float,
) -> list[RawrPlayerSeasonValueRow]:
    records = build_rawr_refresh_records(
        seasons=seasons,
        teams=teams,
        ridge_alpha=rawr_ridge_alpha,
    )
    return [_build_rawr_store_row_from_record(record) for record in records]


def _build_rawr_store_row_from_record(
    record: RawrPlayerSeasonRecord,
) -> RawrPlayerSeasonValueRow:
    return RawrPlayerSeasonValueRow(
        season_id=record.season.year_string_nba_api,
        player_id=record.player.player_id,
        player_name=record.player.player_name,
        games=record.games,
        coefficient=record.coefficient,
        average_minutes=record.minutes.average_minutes,
        total_minutes=record.minutes.total_minutes,
    )
