from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

from wowy.types import GameRecord, NormalizedGamePlayerRecord, NormalizedGameRecord


WOWY_HEADER = ["game_id", "team", "margin", "players"]


def derive_wowy_games(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> list[GameRecord]:
    players_by_game_team: dict[tuple[str, str], set[int]] = defaultdict(set)

    for player in game_players:
        if not player.appeared:
            continue
        players_by_game_team[(player.game_id, player.team)].add(player.player_id)

    derived_games: list[GameRecord] = []

    for game in games:
        players = players_by_game_team.get((game.game_id, game.team), set())
        if not players:
            raise ValueError(
                f"No appeared players found for game {game.game_id!r} and team {game.team!r}"
            )
        derived_games.append(
            GameRecord(
                game_id=game.game_id,
                team=game.team,
                margin=game.margin,
                players=players,
            )
        )

    return derived_games


def write_wowy_games_csv(
    csv_path: Path | str,
    games: list[GameRecord],
) -> None:
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=WOWY_HEADER)
        writer.writeheader()
        for game in games:
            writer.writerow(
                {
                    "game_id": game.game_id,
                    "team": game.team,
                    "margin": game.margin,
                    "players": ";".join(str(player) for player in sorted(game.players)),
                }
            )
