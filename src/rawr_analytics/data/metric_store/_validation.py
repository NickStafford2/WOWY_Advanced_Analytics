from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass

from rawr_analytics.data._validation import (
    validate_iso_datetime,
    validate_optional_non_negative_float,
    validate_optional_non_negative_int,
    validate_required_text,
)
from rawr_analytics.data.metric_store._catalog import MetricScopeCatalogRow
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
)
from rawr_analytics.data.metric_store.full_span import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
)
from rawr_analytics.data.metric_store_scope import validate_metric_scope
from rawr_analytics.shared.season import Season, SeasonType, require_normalized_seasons
from rawr_analytics.shared.team import canonicalize_metric_team_filter


@dataclass
class _MetricRowBatchState:
    row_keys: set[tuple[str, int]]
    scope_season_ids: set[str]
    scope_season_type: SeasonType


def validate_rawr_rows(
    *,
    scope_key: str,
    team_filter: str,
    seasons: list[Season],
    build_version: str,
    source_fingerprint: str,
    rows: list[RawrPlayerSeasonValueRow],
) -> None:
    validate_required_text("rawr", "metric_id")
    validate_required_text(scope_key, "scope_key")
    validate_required_text(build_version, "build_version")
    validate_required_text(source_fingerprint, "source_fingerprint")
    _validate_metric_row_batch_scope(
        scope_key=scope_key,
        team_filter=team_filter,
        seasons=seasons,
    )

    normalized_seasons = require_normalized_seasons(seasons)
    state = _MetricRowBatchState(
        row_keys=set(),
        scope_season_ids={season.year_string_nba_api for season in normalized_seasons},
        scope_season_type=normalized_seasons[0].season_type,
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
    scope_key: str,
    team_filter: str,
    seasons: list[Season],
    build_version: str,
    source_fingerprint: str,
    rows: list[WowyPlayerSeasonValueRow],
) -> None:
    validate_required_text(metric_id, "metric_id")
    validate_required_text(scope_key, "scope_key")
    validate_required_text(build_version, "build_version")
    validate_required_text(source_fingerprint, "source_fingerprint")
    _validate_metric_row_batch_scope(
        scope_key=scope_key,
        team_filter=team_filter,
        seasons=seasons,
    )

    normalized_seasons = require_normalized_seasons(seasons)
    state = _MetricRowBatchState(
        row_keys=set(),
        scope_season_ids={season.year_string_nba_api for season in normalized_seasons},
        scope_season_type=normalized_seasons[0].season_type,
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
    row_season = Season.parse(row.season_id, state.scope_season_type.value)
    assert row_season.season_type == state.scope_season_type, (
        "metric row season parsing must preserve scope season_type"
    )
    canonical_season_id = row_season.year_string_nba_api
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
    scope_key: str,
    team_filter: str,
    seasons: list[Season],
) -> None:
    validate_required_text(scope_key, "scope_key")
    normalized_seasons = require_normalized_seasons(seasons)
    canonical_team_filter = canonicalize_metric_team_filter(team_filter)
    if team_filter != canonical_team_filter:
        raise ValueError(f"Metric row batch uses non-canonical team_filter {team_filter!r}")
    validate_metric_scope(
        scope_key=scope_key,
        team_filter=canonical_team_filter,
        seasons=normalized_seasons,
    )


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


def validate_metric_scope_catalog_row(row: MetricScopeCatalogRow) -> None:
    validate_required_text(row.metric_id, "metric_id")
    validate_required_text(row.scope_key, "scope_key")
    validate_required_text(row.label, "label")
    _validate_metric_catalog(
        scope_key=row.scope_key,
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
    scope_key: str,
    team_filter: str,
    season_type: str,
    available_seasons: list[str],
    available_team_ids: list[int],
    full_span_start_season: str | None,
    full_span_end_season: str | None,
) -> None:
    canonical_season_type = SeasonType.parse(season_type)
    scope_seasons = [
        Season.parse(season, canonical_season_type.value) for season in available_seasons
    ]
    invalid_seasons = [
        season.id for season in scope_seasons if season.season_type != canonical_season_type
    ]
    if invalid_seasons:
        raise ValueError(
            f"Catalog available_seasons must match catalog season_type: {invalid_seasons!r}"
        )
    canonical_seasons = [season.year_string_nba_api for season in scope_seasons]
    validate_metric_scope(
        scope_key=scope_key,
        team_filter=team_filter,
        seasons=scope_seasons,
    )
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

    start = Season.parse(
        full_span_start_season,
        canonical_season_type.value,
    ).year_string_nba_api
    end = Season.parse(
        full_span_end_season or "",
        canonical_season_type.value,
    ).year_string_nba_api
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
    parsed_season = Season.parse(season, season_type.value)
    assert parsed_season.season_type == season_type
    return parsed_season.start_year


def validate_metric_full_span_rows(
    *,
    metric_id: str,
    scope_key: str,
    season_type: str,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    validate_required_text(metric_id, "metric_id")
    validate_required_text(scope_key, "scope_key")
    canonical_season_type = SeasonType.parse(season_type)
    if not series_rows and point_rows:
        raise ValueError("Full-span points require matching series rows")

    ranks: list[int] = []
    expected_point_counts: dict[int, int] = {}

    for row in series_rows:
        if row.metric_id != metric_id or row.scope_key != scope_key:
            raise ValueError("Full-span series rows must match the requested metric scope")
        if row.player_id <= 0:
            raise ValueError(f"Full-span series row has invalid player_id {row.player_id!r}")
        validate_required_text(
            row.player_name,
            f"full-span player_name for player {row.player_id}",
        )
        if not math.isfinite(row.span_average_value):
            raise ValueError(
                f"Full-span series row for player {row.player_id!r} has non-finite value"
            )
        if row.season_count <= 0:
            raise ValueError(
                f"Full-span series row for player {row.player_id!r} has invalid season_count"
            )
        if row.rank_order <= 0:
            raise ValueError(
                f"Full-span series row for player {row.player_id!r} has invalid rank_order"
            )
        if row.player_id in expected_point_counts:
            raise ValueError(f"Duplicate full-span series row for player {row.player_id!r}")

        expected_point_counts[row.player_id] = row.season_count
        ranks.append(row.rank_order)
    if sorted(ranks) != list(range(1, len(series_rows) + 1)):
        raise ValueError("Full-span series rank_order values must be unique and contiguous")

    points_by_player: dict[int, set[str]] = defaultdict(set)
    for row in point_rows:
        if row.metric_id != metric_id or row.scope_key != scope_key:
            raise ValueError("Full-span point rows must match the requested metric scope")
        if row.player_id not in expected_point_counts:
            raise ValueError(f"Full-span point row for unknown player {row.player_id!r}")
        canonical_season_id = Season.parse(
            row.season_id,
            canonical_season_type.value,
        ).year_string_nba_api
        if canonical_season_id != row.season_id:
            raise ValueError(
                f"Full-span point row for player {row.player_id!r} uses "
                f"non-canonical season_id {row.season_id!r}"
            )
        if not math.isfinite(row.value):
            raise ValueError(
                f"Full-span point row for player {row.player_id!r} has non-finite value"
            )
        if row.season_id in points_by_player[row.player_id]:
            raise ValueError(
                f"Duplicate full-span point row for player {row.player_id!r} "
                f"and season_id {row.season_id!r}"
            )
        points_by_player[row.player_id].add(row.season_id)

    for player_id, season_count in expected_point_counts.items():
        if len(points_by_player[player_id]) != season_count:
            raise ValueError(
                f"Full-span player {player_id!r} expected {season_count} "
                f"season points but found {len(points_by_player[player_id])}"
            )


__all__ = [
    "validate_metric_full_span_rows",
    "validate_metric_scope_catalog_row",
    "validate_rawr_rows",
    "validate_wowy_rows",
]
