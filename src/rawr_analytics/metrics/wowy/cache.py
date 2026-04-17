from __future__ import annotations

from rawr_analytics.data.game_cache.store import (
    list_cached_scopes,
    load_games_for_team_season_scopes,
)
from rawr_analytics.metrics.wowy.progress import WowyProgressSink, emit_wowy_progress
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def load_wowy_records(
    *,
    teams: list[Team],
    seasons: list[Season],
    progress_sink: WowyProgressSink | None = None,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    assert seasons, "WOWY record loading requires a non-empty season list"
    assert teams, "WOWY record loading requires a non-empty team list"

    emit_wowy_progress(
        progress_sink,
        phase="resolve",
        current=0,
        total=1,
        detail="Matching query filters to cached team-season scope.",
    )
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
    emit_wowy_progress(
        progress_sink,
        phase="resolve",
        current=1,
        total=1,
        detail=f"Matched {len(team_seasons)} cached team-seasons.",
    )
    if not team_seasons:
        raise ValueError("No cached team-season scopes matched the requested WOWY scope")

    emit_wowy_progress(
        progress_sink,
        phase="db-load",
        current=0,
        total=1,
        detail=f"Loading normalized rows for {len(team_seasons)} cached team-seasons.",
    )
    games, game_players = load_games_for_team_season_scopes(team_seasons)
    emit_wowy_progress(
        progress_sink,
        phase="db-load",
        current=1,
        total=1,
        detail=f"Loaded {len(games)} game rows and {len(game_players)} player rows.",
    )
    return games, game_players
