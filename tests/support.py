from __future__ import annotations

from typing import TypeAlias

from wowy.data.game_cache_db import replace_team_season_normalized_rows
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord


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
    )


def seed_db_from_team_seasons(
    db_path,
    team_seasons: list[TeamSeasonSeed],
) -> None:
    for team, season, games, game_players in team_seasons:
        replace_team_season_normalized_rows(
            db_path,
            team=team,
            season=season,
            season_type="Regular Season",
            games=games,
            game_players=game_players,
            source_path=f"test://{team}_{season}",
            source_snapshot="test-seed",
            source_kind="test",
        )
