from __future__ import annotations

from rawr_analytics.shared.season import Season, normalize_seasons
from rawr_analytics.shared.team import Team, canonicalize_metric_team_filter, to_normalized_team_ids

__all__ = [
    "build_scope_key",
    "build_team_filter",
    "season_ids",
    "validate_metric_scope",
]


def build_team_filter(teams: list[Team] | None) -> str:
    team_ids = to_normalized_team_ids(teams) or []
    return ",".join(str(team_id) for team_id in team_ids)


def build_scope_key(
    *,
    seasons: list[Season],
    teams: list[Team] | None = None,
    team_filter: str | None = None,
) -> str:
    resolved_team_filter = team_filter if team_filter is not None else build_team_filter(teams)
    return (
        f"team_ids={resolved_team_filter or 'all-teams'}"
        f"|seasons={_encode_scope_seasons(seasons)}"
    )


def validate_metric_scope(
    *,
    scope_key: str,
    team_filter: str,
    seasons: list[Season],
) -> None:
    if not seasons:
        raise ValueError("metric scope validation requires non-empty seasons")
    canonical_team_filter = canonicalize_metric_team_filter(team_filter)
    expected_scope_key = build_scope_key(
        team_filter=canonical_team_filter,
        seasons=seasons,
    )
    if scope_key != expected_scope_key:
        raise ValueError(
            f"Invalid scope_key {scope_key!r}; expected canonical {expected_scope_key!r}"
        )


def season_ids(seasons: list[Season]) -> list[str]:
    assert seasons, "metric store reads require a non-empty season filter"
    normalized_seasons = normalize_seasons(seasons)
    assert normalized_seasons is not None, "metric store reads require normalized seasons"
    return sorted({season.year_string_nba_api for season in normalized_seasons})


def _encode_scope_seasons(seasons: list[Season]) -> str:
    normalized_seasons = normalize_seasons(seasons)
    assert normalized_seasons is not None, "metric scope keys require non-empty seasons"
    return ",".join(season.id for season in normalized_seasons)
