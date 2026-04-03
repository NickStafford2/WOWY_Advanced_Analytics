from __future__ import annotations

from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def normalize_query_teams(teams: list[Team] | None) -> list[Team] | None:
    normalized_teams = sorted(
        {team.team_id: team for team in teams or []}.values(),
        key=lambda team: team.team_id,
    )
    return normalized_teams or None


def normalize_query_seasons(seasons: list[Season] | None) -> list[Season] | None:
    normalized_seasons = sorted(
        {(season.start_year, season.season_type): season for season in seasons or []}.values(),
        key=lambda season: (season.id, season.season_type.value),
    )
    return normalized_seasons or None
