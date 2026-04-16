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
from rawr_analytics.shared.season import Season, SeasonType, require_normalized_seasons
from rawr_analytics.shared.team import canonicalize_metric_team_filter


@dataclass
class _MetricRowBatchState:
    row_keys: set[tuple[str, int]]
    scope_season_ids: set[str]


def validate_rawr_rows(
    *,
    metric_cache_key: str,
    team_filter: str,
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
        team_filter=team_filter,
        seasons=seasons,
    )

    normalized_seasons = require_normalized_seasons(seasons)
    state = _MetricRowBatchState(
        row_keys=set(),
        scope_season_ids={season.id for season in normalized_seasons},
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
    team_filter: str,
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
        team_filter=team_filter,
        seasons=seasons,
    )

    normalized_seasons = require_normalized_seasons(seasons)
    state = _MetricRowBatchState(
        row_keys=set(),
        scope_season_ids={season.id for season in normalized_seasons},
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
    if canonical_season_id not in state.scope_season_ids:
        raise ValueError(
            f"Metric row for player {player_id!r} is outside scope seasons: {row.season_id!r}"
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
    team_filter: str,
    seasons: list[Season],
) -> None:
    validate_required_text(metric_cache_key, "metric_cache_key")
    normalized_seasons = require_normalized_seasons(seasons)
    canonical_team_filter = canonicalize_metric_team_filter(team_filter)
    if team_filter != canonical_team_filter:
        raise ValueError(f"Metric row batch uses non-canonical team_filter {team_filter!r}")
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
    validate_required_text(row.label, "label")
    _validate_metric_catalog(
        metric_cache_key=row.metric_cache_key,
        team_filter=row.team_filter,
        season_type=row.season_type,
        available_seasons=row.available_season_ids,
        available_team_ids=row.available_team_ids,
        full_span_start_season=row.full_span_start_season_id,
        full_span_end_season=row.full_span_end_season_id,
    )
    validate_iso_datetime(row.updated_at, "catalog updated_at")


def _validate_metric_catalog(
    *,
    metric_cache_key: str,
    team_filter: str,
    season_type: str,
    available_seasons: list[str],
    available_team_ids: list[int],
    full_span_start_season: str | None,
    full_span_end_season: str | None,
) -> None:
    canonical_season_type = SeasonType.parse(season_type)
    scope_seasons = [Season.parse_id(season) for season in available_seasons]
    invalid_seasons = [
        season.id for season in scope_seasons if season.season_type != canonical_season_type
    ]
    if invalid_seasons:
        raise ValueError(
            f"Catalog available_seasons must match catalog season_type: {invalid_seasons!r}"
        )
    canonical_seasons = [season.id for season in scope_seasons]
    if canonical_seasons != available_seasons:
        raise ValueError("Catalog available_seasons must use canonical season strings")
    if canonical_seasons != sorted(
        set(canonical_seasons),
        key=lambda season: _season_sort_key(season, canonical_season_type),
    ):
        raise ValueError("Catalog available_seasons must be unique and sorted")

    canonical_team_ids = [_canonical_team_id(team_id) for team_id in available_team_ids]
    if canonical_team_ids != available_team_ids:
        raise ValueError("Catalog available_team_ids must use canonical positive team ids")
    if canonical_team_ids != sorted(set(canonical_team_ids)):
        raise ValueError("Catalog available_team_ids must be unique and sorted")

    if (full_span_start_season is None) != (full_span_end_season is None):
        raise ValueError("Catalog full-span seasons must both be set or both be null")
    if full_span_start_season is None:
        return

    start = Season.parse_id(full_span_start_season).id
    end = Season.parse_id(full_span_end_season or "").id
    if start not in canonical_seasons or end not in canonical_seasons:
        raise ValueError("Catalog full-span seasons must be present in available_seasons")
    if _season_sort_key(start, canonical_season_type) > _season_sort_key(
        end,
        canonical_season_type,
    ):
        raise ValueError("Catalog full-span start season must not be after end season")


def _canonical_team_id(value: int) -> int:
    if value <= 0:
        raise ValueError(f"Invalid team_id {value!r}")
    return value


def _season_sort_key(season: str, season_type: SeasonType) -> int:
    parsed_season = Season.parse_id(season)
    assert parsed_season.season_type == season_type
    return parsed_season.start_year
__all__ = [
    "validate_metric_cache_catalog_row",
    "validate_rawr_rows",
    "validate_wowy_rows",
]
