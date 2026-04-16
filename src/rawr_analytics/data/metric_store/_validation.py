from __future__ import annotations

import math
from dataclasses import dataclass

from rawr_analytics.data._validation import (
    validate_iso_datetime,
    validate_optional_non_negative_float,
    validate_optional_non_negative_int,
    validate_required_text,
)
from rawr_analytics.data.metric_store._catalog import MetricCacheCatalogRow
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
)
from rawr_analytics.shared.season import Season, require_normalized_seasons


@dataclass
class _MetricRowBatchState:
    row_keys: set[tuple[str, int]]
    season_ids: set[str]


def validate_rawr_rows(
    *,
    metric_cache_key: str,
    seasons: list[Season],
    build_version: str,
    source_fingerprint: str,
    rows: list[RawrPlayerSeasonValueRow],
) -> None:
    validate_required_text("rawr", "metric_id")
    validate_required_text(metric_cache_key, "metric_cache_key")
    validate_required_text(build_version, "build_version")
    validate_required_text(source_fingerprint, "source_fingerprint")
    _validate_metric_row_batch_scope(
        metric_cache_key=metric_cache_key,
        seasons=seasons,
    )

    normalized_seasons = require_normalized_seasons(seasons)
    state = _MetricRowBatchState(
        row_keys=set(),
        season_ids={season.id for season in normalized_seasons},
    )
    for row in rows:
        _validate_common_metric_row(
            row=row,
            state=state,
        )
        _validate_rawr_value_row(row)


def validate_wowy_rows(
    *,
    metric_id: str,
    metric_cache_key: str,
    seasons: list[Season],
    build_version: str,
    source_fingerprint: str,
    rows: list[WowyPlayerSeasonValueRow],
) -> None:
    validate_required_text(metric_id, "metric_id")
    validate_required_text(metric_cache_key, "metric_cache_key")
    validate_required_text(build_version, "build_version")
    validate_required_text(source_fingerprint, "source_fingerprint")
    _validate_metric_row_batch_scope(
        metric_cache_key=metric_cache_key,
        seasons=seasons,
    )

    normalized_seasons = require_normalized_seasons(seasons)
    state = _MetricRowBatchState(
        row_keys=set(),
        season_ids={season.id for season in normalized_seasons},
    )
    for row in rows:
        _validate_common_metric_row(
            row=row,
            state=state,
        )
        _validate_wowy_value_row(row)


def _validate_common_metric_row(
    *,
    row: RawrPlayerSeasonValueRow | WowyPlayerSeasonValueRow,
    state: _MetricRowBatchState,
) -> None:
    player_id = row.player_id
    canonical_season_id = Season.parse_id(row.season_id).id
    if canonical_season_id != row.season_id:
        raise ValueError(
            f"Metric row for player {player_id!r} uses non-canonical season_id {row.season_id!r}"
        )
    if canonical_season_id not in state.season_ids:
        raise ValueError(
            f"Metric row for player {player_id!r} is outside cached seasons: {row.season_id!r}"
        )

    if row.player_id <= 0:
        raise ValueError(f"Metric row has invalid player_id {row.player_id!r}")
    validate_required_text(
        row.player_name,
        f"player_name for player {row.player_id}",
    )
    validate_optional_non_negative_float(
        row.average_minutes,
        f"average_minutes for player {row.player_id}",
    )
    validate_optional_non_negative_float(
        row.total_minutes,
        f"total_minutes for player {row.player_id}",
    )
    if (
        row.average_minutes is not None
        and row.total_minutes is not None
        and row.total_minutes + 1e-9 < row.average_minutes
    ):
        raise ValueError(
            f"Metric row for player {row.player_id!r} has total_minutes "
            "smaller than average_minutes"
        )
    row_key = (row.season_id, row.player_id)
    if row_key in state.row_keys:
        raise ValueError(f"Duplicate metric row for {row_key!r}")
    state.row_keys.add(row_key)


def _validate_metric_row_batch_scope(
    *,
    metric_cache_key: str,
    seasons: list[Season],
) -> None:
    validate_required_text(metric_cache_key, "metric_cache_key")
    normalized_seasons = require_normalized_seasons(seasons)
    if not normalized_seasons:
        raise ValueError("Metric row batch requires non-empty seasons")


def _validate_rawr_value_row(row: RawrPlayerSeasonValueRow) -> None:
    if not math.isfinite(row.coefficient):
        raise ValueError(f"Metric row for player {row.player_id!r} has non-finite value")
    validate_optional_non_negative_int(
        row.games,
        f"games for player {row.player_id}",
    )


def _validate_wowy_value_row(row: WowyPlayerSeasonValueRow) -> None:
    if row.value is None or not math.isfinite(row.value):
        raise ValueError(f"Metric row for player {row.player_id!r} has non-finite value")
    validate_optional_non_negative_int(
        row.games_with,
        f"games_with for player {row.player_id}",
    )
    validate_optional_non_negative_int(
        row.games_without,
        f"games_without for player {row.player_id}",
    )
    if row.avg_margin_with is None or not math.isfinite(row.avg_margin_with):
        raise ValueError(f"Metric row for player {row.player_id!r} has non-finite avg_margin_with")
    if row.avg_margin_without is None or not math.isfinite(row.avg_margin_without):
        raise ValueError(
            f"Metric row for player {row.player_id!r} has non-finite avg_margin_without"
        )
    if row.raw_wowy_score is not None and not math.isfinite(row.raw_wowy_score):
        raise ValueError(f"Metric row for player {row.player_id!r} has non-finite raw_wowy_score")


def validate_metric_cache_catalog_row(row: MetricCacheCatalogRow) -> None:
    validate_required_text(row.metric_id, "metric_id")
    validate_required_text(row.metric_cache_key, "metric_cache_key")
    _validate_metric_catalog_seasons(row.season_ids)
    validate_iso_datetime(row.updated_at, "catalog updated_at")


def _validate_metric_catalog_seasons(season_ids: list[str]) -> None:
    seasons = [Season.parse_id(season_id) for season_id in season_ids]
    canonical_season_ids = [season.id for season in seasons]
    if canonical_season_ids != season_ids:
        raise ValueError("Catalog season_ids must use canonical season strings")
    if canonical_season_ids != sorted(set(canonical_season_ids)):
        raise ValueError("Catalog season_ids must be unique and sorted")
__all__ = [
    "validate_metric_cache_catalog_row",
    "validate_rawr_rows",
    "validate_wowy_rows",
]
