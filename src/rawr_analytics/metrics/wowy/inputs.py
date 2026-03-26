from __future__ import annotations

from rawr_analytics.data.scope_resolver import load_normalized_scope_records
from rawr_analytics.metrics.wowy.derive import derive_wowy_games
from rawr_analytics.metrics.wowy.models import WowyGameRecord

__all__ = ["load_wowy_game_records"]


def load_wowy_game_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    season_type: str = "Regular Season",
) -> tuple[list[WowyGameRecord], dict[int, str]]:
    games, game_players = load_normalized_scope_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        include_opponents_for_team_scope=False,
    )
    player_names = {player.player_id: player.player_name for player in game_players}
    return derive_wowy_games(games, game_players), player_names
