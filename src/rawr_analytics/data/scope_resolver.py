from __future__ import annotations

from rawr_analytics.data.game_cache.repository import list_cached_team_seasons
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

__all__ = [
    "resolve_team_seasons",
]


def resolve_team_seasons(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    season_type: SeasonType | None = None,
) -> list[TeamSeasonScope]:
    normalized_team_ids = sorted({team_id for team_id in team_ids or [] if team_id > 0}) or None
    normalized_seasons = sorted(dict.fromkeys(seasons or [])) or None
    normalized_teams = [team.strip().upper() for team in teams or [] if team.strip()] or None
    normalized_season_type = _normalize_season_type(season_type)

    cached_team_seasons = [
        scope
        for scope in list_cached_team_seasons()
        if scope.season.season_type == normalized_season_type
    ]

    if normalized_seasons is None and normalized_team_ids is None and normalized_teams is None:
        return cached_team_seasons

    if normalized_seasons is None:
        return [
            scope
            for scope in cached_team_seasons
            if (
                (normalized_team_ids is None or scope.team.team_id in normalized_team_ids)
                and (
                    normalized_teams is None
                    or scope.team.abbreviation(season=scope.season) in normalized_teams
                )
            )
        ]

    scopes_by_key: dict[tuple[int, str], TeamSeasonScope] = {
        (scope.team.team_id, scope.season.id): scope for scope in cached_team_seasons
    }
    resolved: list[TeamSeasonScope] = []
    for season_id in normalized_seasons:
        season = Season(season_id, normalized_season_type.to_nba_format())
        if normalized_team_ids is not None:
            for team_id in normalized_team_ids:
                resolved.append(
                    scopes_by_key.get(
                        (team_id, season.id),
                        TeamSeasonScope(team=Team.from_id(team_id), season=season),
                    )
                )
            continue
        if normalized_teams is not None:
            for abbreviation in normalized_teams:
                team = Team.from_abbreviation(abbreviation, season=season)
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
