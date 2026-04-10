from __future__ import annotations

from rawr_analytics.data.game_cache.store import load_team_season_cache

# from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def load_wowy_records(
    *,
    teams: list[Team],
    seasons: list[Season],
    # season_type: SeasonType,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    assert seasons, "WOWY record loading requires a non-empty season list"
    assert teams, "WOWY record loading requires a non-empty team list"
    # team_seasons = resolve_team_seasons(teams, seasons)
    # if not team_seasons:
    #     raise ValueError("No cached data matched the requested scope")
    #
    team_seasons: list[TeamSeasonScope] = []
    for team in teams:
        for season in seasons:
            team_seasons.append(TeamSeasonScope(team, season))
    return load_team_season_cache(team_seasons)
