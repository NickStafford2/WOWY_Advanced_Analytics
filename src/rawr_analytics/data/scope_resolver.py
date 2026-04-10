from __future__ import annotations

from rawr_analytics.data.game_cache.store import list_cached_scopes
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, require_normalized_seasons
from rawr_analytics.shared.team import Team, normalize_teams

__all__ = [
    "resolve_team_seasons",
]


def resolve_team_seasons(
    teams: list[Team] | None,
    seasons: list[Season],
) -> list[TeamSeasonScope]:
    assert seasons, "team-season resolution requires a non-empty season list"
    normalized_teams = normalize_teams(teams)
    normalized_seasons = require_normalized_seasons(seasons=seasons)

    cached_requested_team_seasons = list_cached_scopes(
        teams=normalized_teams,
        seasons=normalized_seasons,
    )
    if normalized_teams is None:
        return _build_cached_requested_season_scopes(
            seasons=normalized_seasons,
            cached_team_seasons=cached_requested_team_seasons,
        )
    return _build_requested_or_missing_team_seasons(
        teams=normalized_teams,
        seasons=normalized_seasons,
        cached_team_seasons=cached_requested_team_seasons,
    )


def _build_cached_requested_season_scopes(
    *,
    seasons: list[Season],
    cached_team_seasons: list[TeamSeasonScope],
) -> list[TeamSeasonScope]:
    cached_scopes_by_season_id: dict[str, list[TeamSeasonScope]] = {}
    for scope in cached_team_seasons:
        cached_scopes_by_season_id.setdefault(scope.season.id, []).append(scope)
    return [scope for season in seasons for scope in cached_scopes_by_season_id.get(season.id, [])]


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
