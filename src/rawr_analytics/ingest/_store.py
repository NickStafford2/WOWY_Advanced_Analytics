from __future__ import annotations

from rawr_analytics.data.game_cache import replace_team_season_normalized_rows
from rawr_analytics.data.game_cache.rows import NormalizedGamePlayerRow, NormalizedGameRow
from rawr_analytics.ingest._models import IngestResult
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord


def store_team_season(result: IngestResult) -> None:
    replace_team_season_normalized_rows(
        team=result.request.team,
        season=result.request.season,
        games=[_build_game_cache_game_row(game) for game in result.games],
        game_players=[_build_game_cache_game_player_row(player) for player in result.game_players],
        source_path=(
            "sqlite://normalized_games/"
            f"{result.request.team.abbreviation(season=result.request.season)}_"
            f"{result.request.season.id}_{result.request.season.season_type.to_slug()}"
        ),
        source_snapshot="ingest-build-v2",
        source_kind="nba_api",
        expected_games_row_count=result.summary.total_games,
        skipped_games_row_count=0,
    )


def _build_game_cache_game_row(game: NormalizedGameRecord) -> NormalizedGameRow:
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


def _build_game_cache_game_player_row(
    player: NormalizedGamePlayerRecord,
) -> NormalizedGamePlayerRow:
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
