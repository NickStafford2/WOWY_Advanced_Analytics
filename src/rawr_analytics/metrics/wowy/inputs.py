from __future__ import annotations

from pathlib import Path

from rawr_analytics.data.player_metrics_db.constants import DEFAULT_PLAYER_METRICS_DB_PATH
from rawr_analytics.metrics.wowy.derive import derive_wowy_games
from rawr_analytics.metrics.wowy.models import WowyGameRecord
from rawr_analytics.nba.prepare import load_normalized_scope_records

__all__ = ["load_wowy_game_records"]


def load_wowy_game_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    season_type: str = "Regular Season",
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> tuple[list[WowyGameRecord], dict[int, str]]:
    games, game_players = load_normalized_scope_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
    )
    player_names = {player.player_id: player.player_name for player in game_players}
    return derive_wowy_games(games, game_players), player_names
