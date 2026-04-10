from __future__ import annotations

from rawr_analytics.data.game_cache import list_cached_scopes
from rawr_analytics.shared.season import Season, SeasonType, normalize_seasons
from rawr_analytics.shared.team import Team, normalize_teams


def resolve_query_seasons(
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
) -> list[Season]:
    normalized_seasons = normalize_seasons(seasons)
    if normalized_seasons is not None:
        _validate_query_season_type(seasons=normalized_seasons, season_type=season_type)
        return normalized_seasons

    cached_scopes = list_cached_scopes(teams=normalize_teams(teams))
    return normalize_seasons(
        [scope.season for scope in cached_scopes if scope.season.season_type == season_type]
    ) or []


def _validate_query_season_type(
    *,
    seasons: list[Season],
    season_type: SeasonType,
) -> None:
    invalid_seasons = [season.id for season in seasons if season.season_type != season_type]
    assert not invalid_seasons, (
        "Mixed season types are not supported by the current metric query boundary: "
        f"{invalid_seasons!r}"
    )
