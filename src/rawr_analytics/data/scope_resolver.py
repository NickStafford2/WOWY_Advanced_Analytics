from __future__ import annotations

from rawr_analytics.data.game_cache import list_cached_scopes, load_cache_snapshot
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

__all__ = [
    "resolve_team_seasons",
]


def resolve_team_seasons(
    teams: list[Team] | None,
    seasons: list[Season] | None,
    *,
    season_type: SeasonType | None = None,
) -> list[TeamSeasonScope]:
    normalized_teams = (
        sorted(
            {team.team_id: team for team in teams or []}.values(),
            key=lambda team: team.team_id,
        )
        or None
    )
    normalized_seasons = (
        sorted(
            {(season.id, season.season_type.value): season for season in seasons or []}.values(),
            key=lambda season: (season.id, season.season_type.value),
        )
        or None
    )
    season_type = season_type or SeasonType.REGULAR

    if normalized_seasons is None and normalized_teams is None:
        return load_cache_snapshot(season_type).scopes

    if normalized_seasons is None:
        normalized_team_ids = {team.team_id for team in normalized_teams or []}
        cached_team_seasons = load_cache_snapshot(season_type).scopes
        return [
            scope
            for scope in cached_team_seasons
            if normalized_teams is None or scope.team.team_id in normalized_team_ids
        ]

    requested_seasons = [
        Season.parse(season.id, season_type.to_nba_format()) for season in normalized_seasons
    ]
    cached_team_seasons = list_cached_scopes(
        teams=normalized_teams,
        seasons=requested_seasons,
    )

    scopes_by_key: dict[tuple[int, str], TeamSeasonScope] = {
        (scope.team.team_id, scope.season.id): scope for scope in cached_team_seasons
    }
    resolved: list[TeamSeasonScope] = []
    for season in requested_seasons:
        if normalized_teams is not None:
            for team in normalized_teams:
                resolved.append(
                    scopes_by_key.get(
                        (team.team_id, season.id),
                        TeamSeasonScope(team=team, season=season),
                    )
                )
            continue
        resolved.extend([scope for scope in cached_team_seasons if scope.season.id == season.id])
    return resolved
