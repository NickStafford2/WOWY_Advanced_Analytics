from __future__ import annotations

from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.calculate.records import WowyPlayerSeasonRecord
from rawr_analytics.metrics.wowy.calculate.shrinkage import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.refresh.records import build_wowy_refresh_records
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def build_wowy_metric_store_rows(
    *,
    metric: Metric,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    seasons: list[Season],
    teams: list[Team] | None,
) -> list[WowyPlayerSeasonValueRow]:
    records = build_wowy_refresh_records(
        season_type=season_type,
        seasons=seasons,
        teams=teams,
    )
    return [
        _build_wowy_store_row_from_record(
            metric=metric,
            record=record,
            scope_key=scope_key,
            team_filter=team_filter,
        )
        for record in records
    ]


def _build_wowy_store_row_from_record(
    *,
    metric: Metric,
    record: WowyPlayerSeasonRecord,
    scope_key: str,
    team_filter: str,
) -> WowyPlayerSeasonValueRow:
    value = record.result.value
    include_raw_wowy_score = False
    if metric == Metric.WOWY_SHRUNK:
        include_raw_wowy_score = True
        value = compute_wowy_shrinkage_score(
            games_with=record.result.games_with,
            games_without=record.result.games_without,
            wowy_score=record.result.value,
            prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
        )
    return WowyPlayerSeasonValueRow(
        snapshot_id=None,
        metric_id=metric.value,
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=record.season.season_type.value,
        season_id=record.season.year_string_nba_api,
        player_id=record.player.player_id,
        player_name=record.player.player_name,
        value=value,
        games_with=record.result.games_with,
        games_without=record.result.games_without,
        avg_margin_with=record.result.avg_margin_with,
        avg_margin_without=record.result.avg_margin_without,
        average_minutes=record.minutes.average_minutes,
        total_minutes=record.minutes.total_minutes,
        raw_wowy_score=record.result.value if include_raw_wowy_score else None,
    )
