from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import datetime

from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
)
from rawr_analytics.shared.season import SeasonType

_TEAM_ID_FILTER_PATTERN = re.compile(r"^[1-9]\d*$")
_SEASON_YEAR_PATTERN = re.compile(r"^\d{4}-\d{2}$")


def _validate_metric_rows(
    *,
    metric: str,
    scope_key: str,
    metric_label: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[PlayerSeasonMetricRow],
) -> None:
    _validate_required_text(metric, "metric")
    _validate_required_text(scope_key, "scope_key")
    _validate_required_text(metric_label, "metric_label")
    _validate_required_text(build_version, "build_version")
    _validate_required_text(source_fingerprint, "source_fingerprint")

    row_keys: set[tuple[str, int]] = set()
    expected_team_filter: str | None = None
    expected_season_type: str | None = None

    for row in rows:
        if row.metric != metric:
            raise ValueError(
                f"Metric row for player {row.player_id!r} has metric {row.metric!r}; "
                f"expected {metric!r}"
            )
        if row.scope_key != scope_key:
            raise ValueError(
                f"Metric row for player {row.player_id!r} has scope_key {row.scope_key!r}; "
                f"expected {scope_key!r}"
            )
        if row.metric_label != metric_label:
            raise ValueError(
                f"Metric row for player {row.player_id!r} has label {row.metric_label!r}; "
                f"expected {metric_label!r}"
            )

        canonical_season_type = canonicalize_season_type(row.season_type)
        if row.season_type != canonical_season_type:
            raise ValueError(
                f"Metric row for player {row.player_id!r} uses non-canonical season_type "
                f"{row.season_type!r}"
            )
        canonical_team_filter = _canonical_team_filter(row.team_filter)
        if row.team_filter != canonical_team_filter:
            raise ValueError(
                f"Metric row for player {row.player_id!r} uses non-canonical team_filter "
                f"{row.team_filter!r}"
            )
        _validate_scope_shape(
            scope_key=row.scope_key,
            team_filter=canonical_team_filter,
            season_type=canonical_season_type,
        )
        canonical_season = canonicalize_season_year_string(row.season)
        if canonical_season != row.season:
            raise ValueError(
                f"Metric row for player {row.player_id!r} uses non-canonical season {row.season!r}"
            )

        if expected_team_filter is None:
            expected_team_filter = canonical_team_filter
        elif canonical_team_filter != expected_team_filter:
            raise ValueError("Metric rows in the same batch must use one canonical team_filter")

        if expected_season_type is None:
            expected_season_type = canonical_season_type
        elif canonical_season_type != expected_season_type:
            raise ValueError("Metric rows in the same batch must use one canonical season_type")

        if row.player_id <= 0:
            raise ValueError(f"Metric row has invalid player_id {row.player_id!r}")
        _validate_required_text(row.player_name, f"player_name for player {row.player_id}")
        if not math.isfinite(row.value):
            raise ValueError(f"Metric row for player {row.player_id!r} has non-finite value")

        _validate_optional_non_negative_int(
            row.sample_size,
            f"sample_size for player {row.player_id}",
        )
        _validate_optional_non_negative_int(
            row.secondary_sample_size,
            f"secondary_sample_size for player {row.player_id}",
        )
        _validate_optional_non_negative_float(
            row.average_minutes,
            f"average_minutes for player {row.player_id}",
        )
        _validate_optional_non_negative_float(
            row.total_minutes,
            f"total_minutes for player {row.player_id}",
        )
        if (
            row.average_minutes is not None
            and row.total_minutes is not None
            and row.total_minutes + 1e-9 < row.average_minutes
        ):
            raise ValueError(
                f"Metric row for player {row.player_id!r} has total_minutes smaller "
                "than average_minutes"
            )
        if row.details is not None and not isinstance(row.details, dict):
            raise ValueError(f"Metric row for player {row.player_id!r} must use a dict for details")

        row_key = (row.season, row.player_id)
        if row_key in row_keys:
            raise ValueError(f"Duplicate metric row for {row_key!r}")
        row_keys.add(row_key)


def _validate_metric_scope_catalog_row(row: MetricScopeCatalogRow) -> None:
    _validate_required_text(row.metric, "metric")
    _validate_required_text(row.scope_key, "scope_key")
    _validate_required_text(row.metric_label, "metric_label")
    canonical_season_type = canonicalize_season_type(row.season_type)
    if row.season_type != canonical_season_type:
        raise ValueError("Catalog season_type must use canonical season type")
    canonical_team_filter = _canonical_team_filter(row.team_filter)
    if row.team_filter != canonical_team_filter:
        raise ValueError("Catalog team_filter must use canonical positive team_ids")
    _validate_scope_shape(
        scope_key=row.scope_key,
        team_filter=canonical_team_filter,
        season_type=canonical_season_type,
    )

    seasons = [canonicalize_season_year_string(season) for season in row.available_seasons]
    if seasons != row.available_seasons:
        raise ValueError("Catalog available_seasons must use canonical season strings")
    if seasons != sorted(set(seasons), key=season_sort_key):
        raise ValueError("Catalog available_seasons must be unique and sorted")

    team_ids = [_canonical_team_id(team_id) for team_id in row.available_team_ids]
    if team_ids != row.available_team_ids:
        raise ValueError("Catalog available_team_ids must use canonical positive team ids")
    if team_ids != sorted(set(team_ids)):
        raise ValueError("Catalog available_team_ids must be unique and sorted")

    if (row.full_span_start_season is None) != (row.full_span_end_season is None):
        raise ValueError("Catalog full-span seasons must both be set or both be null")
    if row.full_span_start_season is not None:
        start = canonicalize_season_year_string(row.full_span_start_season)
        end = canonicalize_season_year_string(row.full_span_end_season or "")
        if start not in seasons or end not in seasons:
            raise ValueError("Catalog full-span seasons must be present in available_seasons")
        if season_sort_key(start) > season_sort_key(end):
            raise ValueError("Catalog full-span start season must not be after end season")

    _validate_iso_datetime(row.updated_at, "catalog updated_at")


def _validate_metric_full_span_rows(
    *,
    metric: str,
    scope_key: str,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    _validate_required_text(metric, "metric")
    _validate_required_text(scope_key, "scope_key")
    if not series_rows and point_rows:
        raise ValueError("Full-span points require matching series rows")

    ranks: list[int] = []
    expected_point_counts: dict[int, int] = {}

    for row in series_rows:
        if row.metric != metric or row.scope_key != scope_key:
            raise ValueError("Full-span series rows must match the requested metric scope")
        if row.player_id <= 0:
            raise ValueError(f"Full-span series row has invalid player_id {row.player_id!r}")
        _validate_required_text(
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
        if row.metric != metric or row.scope_key != scope_key:
            raise ValueError("Full-span point rows must match the requested metric scope")
        if row.player_id not in expected_point_counts:
            raise ValueError(f"Full-span point row for unknown player {row.player_id!r}")
        canonical_season = canonicalize_season_year_string(row.season)
        if canonical_season != row.season:
            raise ValueError(
                f"Full-span point row for player {row.player_id!r} uses non-canonical "
                f"season {row.season!r}"
            )
        if not math.isfinite(row.value):
            raise ValueError(
                f"Full-span point row for player {row.player_id!r} has non-finite value"
            )
        if row.season in points_by_player[row.player_id]:
            raise ValueError(
                f"Duplicate full-span point row for player {row.player_id!r} and "
                f"season {row.season!r}"
            )
        points_by_player[row.player_id].add(row.season)

    for player_id, season_count in expected_point_counts.items():
        if len(points_by_player[player_id]) != season_count:
            raise ValueError(
                f"Full-span player {player_id!r} expected {season_count} season points "
                f"but found {len(points_by_player[player_id])}"
            )


def _validate_scope_shape(*, scope_key: str, team_filter: str, season_type: str) -> None:
    expected_team_key = team_filter or "all-teams"
    expected_scope_key = f"team_ids={expected_team_key}|season_type={season_type}"
    if scope_key != expected_scope_key:
        raise ValueError(
            f"Invalid scope_key {scope_key!r}; expected canonical {expected_scope_key!r}"
        )
    if not team_filter:
        return
    team_ids = team_filter.split(",")
    if team_ids != sorted(set(team_ids), key=int):
        raise ValueError("team_filter must be unique and sorted")
    for team_id in team_ids:
        _canonical_team_id_filter_value(team_id)


def _canonical_team_filter(value: str) -> str:
    if not value:
        return ""
    team_ids = value.split(",")
    canonical_team_ids = [_canonical_team_id_filter_value(team_id) for team_id in team_ids]
    if canonical_team_ids != sorted(set(canonical_team_ids), key=int):
        raise ValueError("team_filter must be unique and sorted")
    return ",".join(canonical_team_ids)


def _canonical_team_id(value: int) -> int:
    if value <= 0:
        raise ValueError(f"Invalid team id {value!r}")
    return value


def _canonical_team_id_filter_value(value: str) -> str:
    team_id = value.strip()
    if not _TEAM_ID_FILTER_PATTERN.fullmatch(team_id):
        raise ValueError(f"Invalid team_id filter value {value!r}")
    return team_id


def _validate_required_text(value: str, label: str) -> None:
    if not value.strip():
        raise ValueError(f"{label} must not be empty")


def _validate_optional_non_negative_int(value: int | None, label: str) -> None:
    if value is None:
        return
    if value < 0:
        raise ValueError(f"{label} must not be negative")


def _validate_optional_non_negative_float(value: float | None, label: str) -> None:
    if value is None:
        return
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"{label} must be a finite non-negative number")


def _validate_iso_datetime(value: str, label: str) -> None:
    _validate_required_text(value, label)
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO datetime") from exc


def canonicalize_season_type(value: str) -> str:
    return SeasonType.parse(value).to_nba_format()


def canonicalize_season_year_string(value: str) -> str:
    season = value.strip()
    if not _SEASON_YEAR_PATTERN.fullmatch(season):
        raise ValueError(f"Invalid season string {value!r}")
    return season


def season_sort_key(value: str) -> tuple[int, str]:
    season = canonicalize_season_year_string(value)
    return (int(season[:4]), season)
