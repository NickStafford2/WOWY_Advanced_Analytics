from __future__ import annotations

import re

from rawr_analytics.shared.season import Season, SeasonType

_TEAM_ID_FILTER_PATTERN = re.compile(r"^[1-9]\d*$")


def validate_metric_scope(*, scope_key: str, team_filter: str, season_type: str) -> None:
    canonical_season_type = canonicalize_metric_season_type(season_type)
    canonical_team_filter = canonicalize_metric_team_filter(team_filter)
    expected_team_key = canonical_team_filter or "all-teams"
    expected_scope_key = f"team_ids={expected_team_key}|season_type={canonical_season_type}"
    if scope_key != expected_scope_key:
        raise ValueError(
            f"Invalid scope_key {scope_key!r}; expected canonical {expected_scope_key!r}"
        )


def validate_metric_catalog(
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

    canonical_seasons = [canonicalize_metric_season(season) for season in available_seasons]
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

    start = canonicalize_metric_season(full_span_start_season)
    end = canonicalize_metric_season(full_span_end_season or "")
    if start not in canonical_seasons or end not in canonical_seasons:
        raise ValueError("Catalog full-span seasons must be present in available_seasons")
    if _season_sort_key(start) > _season_sort_key(end):
        raise ValueError("Catalog full-span start season must not be after end season")


def canonicalize_metric_team_filter(team_filter: str) -> str:
    if not team_filter:
        return ""

    team_ids = team_filter.split(",")
    if team_ids != sorted(set(team_ids), key=int):
        raise ValueError("team_filter must be unique and sorted")
    for team_id in team_ids:
        _canonical_team_id_filter_value(team_id)
    return ",".join(team_ids)


def canonicalize_metric_season_type(season_type: str) -> str:
    return SeasonType.parse(season_type).value


def canonicalize_metric_season(season: str) -> str:
    return Season(season, SeasonType.REGULAR.value).id


def _canonical_team_id(value: int) -> int:
    if value <= 0:
        raise ValueError(f"Invalid team_id {value!r}")
    return value


def _canonical_team_id_filter_value(value: str) -> int:
    if not _TEAM_ID_FILTER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid team_filter value {value!r}")
    return int(value)


def _season_sort_key(season: str) -> int:
    return Season(season, SeasonType.REGULAR.value).start_year
