from __future__ import annotations

from typing import TypeAlias

import rawr_analytics.data.game_cache.repository as game_cache_repository
from rawr_analytics.data.game_cache.repository import replace_team_season_normalized_rows
from rawr_analytics.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.nba.team_identity import resolve_team_id

TeamSeasonSeed: TypeAlias = tuple[
    str,
    str,
    list[NormalizedGameRecord],
    list[NormalizedGamePlayerRecord],
]


def game(
    game_id: str,
    season: str,
    game_date: str,
    team: str,
    opponent: str,
    is_home: bool,
    margin: float,
    season_type: str = "Regular Season",
    source: str = "nba_api",
) -> NormalizedGameRecord:
    team_id = resolve_team_id(team, game_date=game_date)
    return NormalizedGameRecord(
        game_id=game_id,
        season=season,
        game_date=game_date,
        team=team,
        opponent=opponent,
        is_home=is_home,
        margin=margin,
        season_type=season_type,
        source=source,
        team_id=team_id,
        opponent_team_id=resolve_team_id(opponent, game_date=game_date),
    )


def player(
    game_id: str,
    team: str,
    player_id: int,
    player_name: str,
    appeared: bool,
    minutes: float | None,
) -> NormalizedGamePlayerRecord:
    return NormalizedGamePlayerRecord(
        game_id=game_id,
        team=team,
        player_id=player_id,
        player_name=player_name,
        appeared=appeared,
        minutes=minutes,
        team_id=resolve_team_id(team),
    )


def seed_db_from_team_seasons(
    db_path,
    team_seasons: list[TeamSeasonSeed],
) -> None:
    for team, season, games, game_players in team_seasons:
        season_types = {game.season_type for game in games}
        if len(season_types) > 1:
            raise ValueError(f"Expected one season type per team-season seed for {team} {season}")
        original_validate = game_cache_repository.validate_normalized_cache_batch
        game_cache_repository.validate_normalized_cache_batch = lambda **_kwargs: None
        try:
            replace_team_season_normalized_rows(
                db_path,
                team=team,
                team_id=resolve_team_id(team, season=season),
                season=season,
                season_type=season_types.pop() if season_types else "Regular Season",
                games=games,
                game_players=game_players,
                source_path=f"test://{team}_{season}",
                source_snapshot="test-seed",
                source_kind="test",
                expected_games_row_count=len(games),
                skipped_games_row_count=0,
            )
        finally:
            game_cache_repository.validate_normalized_cache_batch = original_validate
