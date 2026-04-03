from __future__ import annotations

from rawr_analytics.data.game_cache import replace_team_season_normalized_rows
from rawr_analytics.data.game_cache.rows import NormalizedGamePlayerRow, NormalizedGameRow
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
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
    replace_team_season_normalized_rows(
        team=team,
        season=season,
        games=[_build_game_row(game) for game in games],
        game_players=[_build_game_player_row(player) for player in game_players],
        source_path=source_path,
        source_snapshot=source_snapshot,
        source_kind="kaggle",
        expected_games_row_count=expected_games_row_count,
        skipped_games_row_count=skipped_games_row_count,
    )


def _build_game_row(game: NormalizedGameRecord) -> NormalizedGameRow:
    return NormalizedGameRow(
        game_id=game.game_id,
        game_date=game.game_date,
        season=game.season,
        team=game.team,
        opponent_team=game.opponent_team,
        is_home=game.is_home,
        margin=game.margin,
        source=game.source,
    )


def _build_game_player_row(player: NormalizedGamePlayerRecord) -> NormalizedGamePlayerRow:
    return NormalizedGamePlayerRow(
        game_id=player.game_id,
        player=player.player,
        appeared=player.appeared,
        minutes=player.minutes,
        team=player.team,
    )


__all__ = [
    "store_team_season",
]
