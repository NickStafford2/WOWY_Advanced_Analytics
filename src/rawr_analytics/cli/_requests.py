from __future__ import annotations

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.services.metric_refresh import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    MetricStoreRefreshRequest,
)
from rawr_analytics.services.rebuild import RebuildRequest
from rawr_analytics.shared.season import SeasonType


def build_rebuild_request(
    *,
    start_year: int,
    end_year: int,
    season_type: str,
    teams: list[str] | None,
    metrics: list[str] | None,
    keep_existing_db: bool,
) -> RebuildRequest:
    if start_year < end_year:
        raise ValueError("Start year must be greater than or equal to end year")
    return RebuildRequest(
        start_year=start_year,
        end_year=end_year,
        season_type=SeasonType.parse(season_type),
        teams=teams,
        metrics=[Metric.parse(metric) for metric in metrics] if metrics else None,
        keep_existing_db=keep_existing_db,
    )


def build_metric_store_refresh_request(
    *,
    metric: str,
    season_type: str,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    include_team_scopes: bool = True,
) -> MetricStoreRefreshRequest:
    return MetricStoreRefreshRequest(
        metric=Metric.parse(metric),
        season_type=SeasonType.parse(season_type),
        rawr_ridge_alpha=rawr_ridge_alpha,
        include_team_scopes=include_team_scopes,
    )
