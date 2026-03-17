from __future__ import annotations

from collections import defaultdict

from wowy.apps.wowy.models import WowyGameRecord
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord


def derive_wowy_games(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> list[WowyGameRecord]:
    players_by_game_team: dict[tuple[str, str], set[int]] = defaultdict(set)

    for player in game_players:
        if not player.appeared:
            continue
        players_by_game_team[(player.game_id, player.team)].add(player.player_id)

    derived_games: list[WowyGameRecord] = []

    for game in games:
        players = players_by_game_team.get((game.game_id, game.team), set())
        if not players:
            raise ValueError(
                f"No appeared players found for game {game.game_id!r} and team {game.team!r}"
            )
        derived_games.append(
            WowyGameRecord(
                game_id=game.game_id,
                season=game.season,
                team=game.team,
                margin=game.margin,
                players=players,
            )
        )

    return derived_games
