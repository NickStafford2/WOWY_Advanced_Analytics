"""Public repository interface for player metric storage."""

from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    MetricStoreMetadata,
)
from rawr_analytics.data.player_metrics_db.queries import (
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
)
from rawr_analytics.data.player_metrics_db.rawr import (
    RawrPlayerSeasonValueRow,
    build_rawr_player_season_value_rows,
    load_rawr_player_season_value_rows,
)
from rawr_analytics.data.player_metrics_db.schema import initialize_player_metrics_db
from rawr_analytics.data.player_metrics_db.store import (
    clear_metric_scope_store,
    replace_rawr_scope_snapshot,
    replace_wowy_scope_snapshot,
)
from rawr_analytics.data.player_metrics_db.wowy import (
    WowyPlayerSeasonValueRow,
    build_wowy_player_season_value_rows,
    load_wowy_player_season_value_rows,
)

__all__ = [
    "MetricFullSpanPointRow",
    "MetricFullSpanSeriesRow",
    "MetricScopeCatalogRow",
    "MetricStoreMetadata",
    "RawrPlayerSeasonValueRow",
    "WowyPlayerSeasonValueRow",
    "build_rawr_player_season_value_rows",
    "build_wowy_player_season_value_rows",
    "clear_metric_scope_store",
    "initialize_player_metrics_db",
    "load_metric_full_span_points_map",
    "load_metric_full_span_series_rows",
    "load_metric_scope_catalog_row",
    "load_metric_store_metadata",
    "load_rawr_player_season_value_rows",
    "load_wowy_player_season_value_rows",
    "replace_rawr_scope_snapshot",
    "replace_wowy_scope_snapshot",
]
