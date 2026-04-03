from __future__ import annotations

from rawr_analytics.data.game_cache import list_cached_team_seasons
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
    normalized_season_type = _normalize_season_type(season_type)

    cached_team_seasons = [
        scope
        for scope in list_cached_team_seasons()
        if scope.season.season_type == normalized_season_type
    ]

    if normalized_seasons is None and normalized_teams is None:
        return cached_team_seasons

    if normalized_seasons is None:
        normalized_team_ids = {team.team_id for team in normalized_teams or []}
        return [
            scope
            for scope in cached_team_seasons
            if normalized_teams is None or scope.team.team_id in normalized_team_ids
        ]

    scopes_by_key: dict[tuple[int, str], TeamSeasonScope] = {
        (scope.team.team_id, scope.season.id): scope for scope in cached_team_seasons
    }
    resolved: list[TeamSeasonScope] = []
    for requested_season in normalized_seasons:
        season = Season(requested_season.id, normalized_season_type.to_nba_format())
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


def _normalize_season_type(season_type: SeasonType | None) -> SeasonType:
    if season_type is None:
        return SeasonType.REGULAR
    return season_type
