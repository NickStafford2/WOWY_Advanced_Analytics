from __future__ import annotations

from rawr_analytics.data.game_cache import list_cached_scopes, load_cache_snapshot
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType, normalize_seasons
from rawr_analytics.shared.team import Team, normalize_teams

__all__ = [
    "resolve_team_seasons",
]


def resolve_team_seasons(
    teams: list[Team] | None,
    seasons: list[Season] | None,
    *,
    season_type: SeasonType | None = None,
) -> list[TeamSeasonScope]:
    normalized_teams = normalize_teams(teams)
    resolved_season_type = season_type or SeasonType.REGULAR
    requested_seasons = _normalize_requested_seasons(
        seasons=seasons,
        season_type=resolved_season_type,
    )

    if requested_seasons is None:
        return _resolve_cached_team_seasons(
            teams=normalized_teams,
            season_type=resolved_season_type,
        )

    cached_requested_team_seasons = list_cached_scopes(
        teams=normalized_teams,
        seasons=requested_seasons,
    )
    if normalized_teams is None:
        return _build_cached_requested_season_scopes(
            seasons=requested_seasons,
            cached_team_seasons=cached_requested_team_seasons,
        )
    return _build_requested_or_missing_team_seasons(
        teams=normalized_teams,
        seasons=requested_seasons,
        cached_team_seasons=cached_requested_team_seasons,
    )
def _normalize_requested_seasons(
    *,
    seasons: list[Season] | None,
    season_type: SeasonType,
) -> list[Season] | None:
    normalized_seasons = normalize_seasons(seasons)
    if normalized_seasons is None:
        return None
    return [
        Season.parse(season.id, season_type.to_nba_format()) for season in normalized_seasons
    ]


def _resolve_cached_team_seasons(
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
) -> list[TeamSeasonScope]:
    cached_team_seasons = load_cache_snapshot(season_type).scopes
    if teams is None:
        return cached_team_seasons
    requested_team_ids = {team.team_id for team in teams}
    return [
        scope for scope in cached_team_seasons if scope.team.team_id in requested_team_ids
    ]
def _build_cached_requested_season_scopes(
    *,
    seasons: list[Season],
    cached_team_seasons: list[TeamSeasonScope],
) -> list[TeamSeasonScope]:
    cached_scopes_by_season_id: dict[str, list[TeamSeasonScope]] = {}
    for scope in cached_team_seasons:
        cached_scopes_by_season_id.setdefault(scope.season.id, []).append(scope)
    return [
        scope
        for season in seasons
        for scope in cached_scopes_by_season_id.get(season.id, [])
    ]


def _build_requested_or_missing_team_seasons(
    *,
    teams: list[Team],
    seasons: list[Season],
    cached_team_seasons: list[TeamSeasonScope],
) -> list[TeamSeasonScope]:
    cached_scopes_by_key: dict[tuple[int, str], TeamSeasonScope] = {
        (scope.team.team_id, scope.season.id): scope for scope in cached_team_seasons
    }
    resolved_scopes: list[TeamSeasonScope] = []
    for season in seasons:
        for team in teams:
            resolved_scopes.append(
                cached_scopes_by_key.get(
                    (team.team_id, season.id),
                    TeamSeasonScope(team=team, season=season),
                )
            )
    return resolved_scopes
