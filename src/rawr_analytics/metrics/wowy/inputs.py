from __future__ import annotations

from rawr_analytics.data.game_cache.repository import load_normalized_scope_records_from_db
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics.wowy.derive import derive_wowy_games
from rawr_analytics.metrics.wowy.models import WowyGameRecord
from rawr_analytics.shared.season import SeasonType

__all__ = ["load_wowy_game_records"]


def load_wowy_game_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    season_type: SeasonType = SeasonType.REGULAR,
) -> tuple[list[WowyGameRecord], dict[int, str]]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        team_ids=team_ids,
        season_type=season_type,
    )
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")
    games, game_players = load_normalized_scope_records_from_db(team_seasons)
    player_names = {player.player_id: player.player_name for player in game_players}
    return derive_wowy_games(games, game_players), player_names
