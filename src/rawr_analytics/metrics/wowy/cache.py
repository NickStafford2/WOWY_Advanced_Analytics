from __future__ import annotations

from rawr_analytics.data.game_cache.store import load_team_season_cache
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def load_wowy_records(
    *,
    teams: list[Team] | None,
    seasons: list[Season],
    season_type: SeasonType,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    assert seasons, "WOWY record loading requires a non-empty season list"
    team_seasons = resolve_team_seasons(teams, seasons, season_type=season_type)
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")
    return load_team_season_cache(team_seasons)
