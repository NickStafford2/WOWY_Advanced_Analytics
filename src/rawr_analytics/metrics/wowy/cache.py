from __future__ import annotations

from rawr_analytics.data.game_cache.store import list_cached_scopes, load_team_season_cache
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def load_wowy_records(
    *,
    teams: list[Team],
    seasons: list[Season],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    assert seasons, "WOWY record loading requires a non-empty season list"
    assert teams, "WOWY record loading requires a non-empty team list"
    cached_scopes_by_key = {
        (scope.team.team_id, scope.season): scope
        for scope in list_cached_scopes(teams=teams, seasons=seasons)
    }
    team_seasons: list[TeamSeasonScope] = []
    for season in seasons:
        for team in teams:
            if not team.is_active_during(season):
                continue
            cached_scope = cached_scopes_by_key.get((team.team_id, season))
            if cached_scope is None:
                continue
            team_seasons.append(cached_scope)
    if not team_seasons:
        raise ValueError("No cached team-season scopes matched the requested WOWY scope")
    return load_team_season_cache(team_seasons)
