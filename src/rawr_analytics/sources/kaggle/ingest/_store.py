from __future__ import annotations

from rawr_analytics.data.game_cache import store_team_season_cache
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def store_team_season(
    *,
    team: Team,
    season: Season,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    source_path: str,
    source_snapshot: str,
    expected_games_row_count: int,
    skipped_games_row_count: int,
) -> None:
    store_team_season_cache(
        scope=TeamSeasonScope(team=team, season=season),
        games=games,
        game_players=game_players,
        source_path=source_path,
        source_snapshot=source_snapshot,
        source_kind="kaggle",
        expected_games_count=expected_games_row_count,
        skipped_games_count=skipped_games_row_count,
    )


__all__ = [
    "store_team_season",
]
