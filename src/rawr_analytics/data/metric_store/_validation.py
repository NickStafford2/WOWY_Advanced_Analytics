from __future__ import annotations

import math
from collections import defaultdict

from rawr_analytics.data._validation import (
    _validate_iso_datetime,
    _validate_optional_non_negative_float,
    _validate_optional_non_negative_int,
    _validate_required_text,
)
from rawr_analytics.data.metric_store.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
)
from rawr_analytics.data.metric_store.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow
from rawr_analytics.data.metric_store_scope import validate_metric_scope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import canonicalize_metric_team_filter


def validate_rawr_rows(
    *,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[RawrPlayerSeasonValueRow],
) -> None:
    _validate_required_text("rawr", "metric_id")
    _validate_required_text(scope_key, "scope_key")
    _validate_required_text(build_version, "build_version")
    _validate_required_text(source_fingerprint, "source_fingerprint")

    row_keys: set[tuple[str, int]] = set()
    expected_team_filter: str | None = None
    expected_season_type: str | None = None
    expected_snapshot_id: int | None = None
    snapshot_id_seen = False

    for row in rows:
        player_id = row.value.player_id
        if row.metric_id != "rawr":
            raise ValueError(
                f"Metric row for player {player_id!r} has metric_id {row.metric_id!r}; "
                "expected 'rawr'"
            )
        if row.scope_key != scope_key:
            raise ValueError(
                f"Metric row for player {player_id!r} has scope_key "
                f"{row.scope_key!r}; expected {scope_key!r}"
            )

        canonical_season_type = SeasonType.parse(row.season_type).value
        if row.season_type != canonical_season_type:
            raise ValueError(
                f"Metric row for player {player_id!r} uses non-canonical "
                f"season_type {row.season_type!r}"
            )
        canonical_team_filter = canonicalize_metric_team_filter(row.team_filter)
        if row.team_filter != canonical_team_filter:
            raise ValueError(
                f"Metric row for player {player_id!r} uses non-canonical "
                f"team_filter {row.team_filter!r}"
            )
        validate_metric_scope(
            scope_key=row.scope_key,
            team_filter=canonical_team_filter,
            season_type=canonical_season_type,
        )
        canonical_season_id = Season(
            row.value.season_id, SeasonType.REGULAR.value
        ).id  # todo. this may be wrong. why regular as parameter?
        if canonical_season_id != row.value.season_id:
            raise ValueError(
                "Metric row for player "
                f"{row.value.player_id!r} uses non-canonical season_id {row.value.season_id!r}"
            )

        if expected_team_filter is None:
            expected_team_filter = canonical_team_filter
        elif canonical_team_filter != expected_team_filter:
            raise ValueError("Metric rows in the same batch must use one canonical team_filter")

        if expected_season_type is None:
            expected_season_type = canonical_season_type
        elif canonical_season_type != expected_season_type:
            raise ValueError("Metric rows in the same batch must use one canonical season_type")

        if row.snapshot_id is not None and row.snapshot_id <= 0:
            raise ValueError(
                f"Metric row for player {player_id!r} has invalid snapshot_id "
                f"{row.snapshot_id!r}"
            )
        if not snapshot_id_seen:
            expected_snapshot_id = row.snapshot_id
            snapshot_id_seen = True
        elif row.snapshot_id != expected_snapshot_id:
            raise ValueError("Metric rows in the same batch must use one snapshot_id")

        if row.value.player_id <= 0:
            raise ValueError(f"Metric row has invalid player_id {row.value.player_id!r}")
        _validate_required_text(
            row.value.player_name,
            f"player_name for player {row.value.player_id}",
        )
        if not math.isfinite(row.value.coefficient):
            raise ValueError(
                f"Metric row for player {row.value.player_id!r} has non-finite value"
            )
        _validate_optional_non_negative_int(
            row.value.games,
            f"games for player {row.value.player_id}",
        )
        _validate_optional_non_negative_float(
            row.value.average_minutes,
            f"average_minutes for player {row.value.player_id}",
        )
        _validate_optional_non_negative_float(
            row.value.total_minutes,
            f"total_minutes for player {row.value.player_id}",
        )
        if (
            row.value.average_minutes is not None
            and row.value.total_minutes is not None
            and row.value.total_minutes + 1e-9 < row.value.average_minutes
        ):
            raise ValueError(
                f"Metric row for player {row.value.player_id!r} has total_minutes "
                "smaller than average_minutes"
            )
        row_key = (row.value.season_id, row.value.player_id)
        if row_key in row_keys:
            raise ValueError(f"Duplicate metric row for {row_key!r}")
        row_keys.add(row_key)


def validate_wowy_rows(
    *,
    metric_id: str,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[WowyPlayerSeasonValueRow],
) -> None:
    _validate_required_text(metric_id, "metric_id")
    _validate_required_text(scope_key, "scope_key")
    _validate_required_text(build_version, "build_version")
    _validate_required_text(source_fingerprint, "source_fingerprint")

    row_keys: set[tuple[str, int]] = set()
    expected_team_filter: str | None = None
    expected_season_type: str | None = None
    expected_snapshot_id: int | None = None
    snapshot_id_seen = False

    for row in rows:
        player_id = row.value.player_id
        if row.metric_id != metric_id:
            raise ValueError(
                f"Metric row for player {player_id!r} has metric_id {row.metric_id!r}; "
                f"expected {metric_id!r}"
            )
        if row.scope_key != scope_key:
            raise ValueError(
                f"Metric row for player {player_id!r} has scope_key "
                f"{row.scope_key!r}; expected {scope_key!r}"
            )

        canonical_season_type = SeasonType.parse(row.season_type).value
        if row.season_type != canonical_season_type:
            raise ValueError(
                f"Metric row for player {player_id!r} uses non-canonical "
                f"season_type {row.season_type!r}"
            )
        canonical_team_filter = canonicalize_metric_team_filter(row.team_filter)
        if row.team_filter != canonical_team_filter:
            raise ValueError(
                f"Metric row for player {player_id!r} uses non-canonical "
                f"team_filter {row.team_filter!r}"
            )
        validate_metric_scope(
            scope_key=row.scope_key,
            team_filter=canonical_team_filter,
            season_type=canonical_season_type,
        )
        canonical_season_id = Season(row.value.season_id, SeasonType.REGULAR.value).id
        if canonical_season_id != row.value.season_id:
            raise ValueError(
                "Metric row for player "
                f"{player_id!r} uses non-canonical season_id {row.value.season_id!r}"
            )

        if expected_team_filter is None:
            expected_team_filter = canonical_team_filter
        elif canonical_team_filter != expected_team_filter:
            raise ValueError("Metric rows in the same batch must use one canonical team_filter")

        if expected_season_type is None:
            expected_season_type = canonical_season_type
        elif canonical_season_type != expected_season_type:
            raise ValueError("Metric rows in the same batch must use one canonical season_type")

        if row.snapshot_id is not None and row.snapshot_id <= 0:
            raise ValueError(
                f"Metric row for player {player_id!r} has invalid snapshot_id "
                f"{row.snapshot_id!r}"
            )
        if not snapshot_id_seen:
            expected_snapshot_id = row.snapshot_id
            snapshot_id_seen = True
        elif row.snapshot_id != expected_snapshot_id:
            raise ValueError("Metric rows in the same batch must use one snapshot_id")

        if player_id <= 0:
            raise ValueError(f"Metric row has invalid player_id {player_id!r}")
        _validate_required_text(row.value.player_name, f"player_name for player {player_id}")
        if not math.isfinite(row.value.value):
            raise ValueError(f"Metric row for player {player_id!r} has non-finite value")
        _validate_optional_non_negative_int(
            row.value.games_with,
            f"games_with for player {player_id}",
        )
        _validate_optional_non_negative_int(
            row.value.games_without,
            f"games_without for player {player_id}",
        )
        if not math.isfinite(row.value.avg_margin_with):
            raise ValueError(
                f"Metric row for player {player_id!r} has non-finite avg_margin_with"
            )
        if not math.isfinite(row.value.avg_margin_without):
            raise ValueError(
                f"Metric row for player {player_id!r} has non-finite avg_margin_without"
            )
        if row.value.raw_wowy_score is not None and not math.isfinite(row.value.raw_wowy_score):
            raise ValueError(
                f"Metric row for player {player_id!r} has non-finite raw_wowy_score"
            )
        _validate_optional_non_negative_float(
            row.value.average_minutes,
            f"average_minutes for player {player_id}",
        )
        _validate_optional_non_negative_float(
            row.value.total_minutes,
            f"total_minutes for player {player_id}",
        )
        if (
            row.value.average_minutes is not None
            and row.value.total_minutes is not None
            and row.value.total_minutes + 1e-9 < row.value.average_minutes
        ):
            raise ValueError(
                f"Metric row for player {player_id!r} has total_minutes "
                "smaller than average_minutes"
            )

        row_key = (row.value.season_id, player_id)
        if row_key in row_keys:
            raise ValueError(f"Duplicate metric row for {row_key!r}")
        row_keys.add(row_key)


def validate_metric_scope_catalog_row(row: MetricScopeCatalogRow) -> None:
    _validate_required_text(row.metric_id, "metric_id")
    _validate_required_text(row.scope_key, "scope_key")
    _validate_required_text(row.label, "label")
    _validate_metric_catalog(
        scope_key=row.scope_key,
        team_filter=row.team_filter,
        season_type=row.season_type,
        available_seasons=row.available_season_ids,
        available_team_ids=row.available_team_ids,
        full_span_start_season=row.full_span_start_season_id,
        full_span_end_season=row.full_span_end_season_id,
    )
    _validate_iso_datetime(row.updated_at, "catalog updated_at")


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
    validate_metric_scope(
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type,
    )

    canonical_seasons = [
        Season(season, SeasonType.REGULAR.value).id for season in available_seasons
    ]
    if canonical_seasons != available_seasons:
        raise ValueError("Catalog available_seasons must use canonical season strings")
    if canonical_seasons != sorted(set(canonical_seasons), key=_season_sort_key):
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

    start = Season(full_span_start_season, SeasonType.REGULAR.value).id
    end = Season(full_span_end_season or "", SeasonType.REGULAR.value).id
    if start not in canonical_seasons or end not in canonical_seasons:
        raise ValueError("Catalog full-span seasons must be present in available_seasons")
    if _season_sort_key(start) > _season_sort_key(end):
        raise ValueError("Catalog full-span start season must not be after end season")


def _canonical_team_id(value: int) -> int:
    if value <= 0:
        raise ValueError(f"Invalid team_id {value!r}")
    return value


def _season_sort_key(season: str) -> int:
    return Season(season, SeasonType.REGULAR.value).start_year


def validate_metric_full_span_rows(
    *,
    metric_id: str,
    scope_key: str,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    _validate_required_text(metric_id, "metric_id")
    _validate_required_text(scope_key, "scope_key")
    if not series_rows and point_rows:
        raise ValueError("Full-span points require matching series rows")

    ranks: list[int] = []
    expected_point_counts: dict[int, int] = {}

    for row in series_rows:
        if row.metric_id != metric_id or row.scope_key != scope_key:
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
        if row.metric_id != metric_id or row.scope_key != scope_key:
            raise ValueError("Full-span point rows must match the requested metric scope")
        if row.player_id not in expected_point_counts:
            raise ValueError(f"Full-span point row for unknown player {row.player_id!r}")
        canonical_season_id = Season(row.season_id, SeasonType.REGULAR.value).id
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
