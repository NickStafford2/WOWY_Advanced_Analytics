from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.shared.team import Team

ProgressFn = Callable[[int, int, str | None], None]


@dataclass(frozen=True)
class WowyGame:
    game_id: str
    margin: float
    players: frozenset[int]
    team: Team


@dataclass(frozen=True)
class WowyPlayerValue:
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    value: float | None
    raw_value: float | None = None


def compute_wowy(
    games: list[WowyGame],
    progress: ProgressFn | None = None,
) -> dict[int, WowyPlayerValue]:
    all_players: set[int] = set()
    for game in games:
        all_players.update(game.players)

    results: dict[int, WowyPlayerValue] = {}
    sorted_players = sorted(all_players)
    total_players = len(sorted_players)
    for index, player_id in enumerate(sorted_players, start=1):
        margins_with: list[float] = []
        margins_without: list[float] = []
        teams_with_player = {game.team.team_id for game in games if player_id in game.players}

        for game in games:
            if game.team.team_id not in teams_with_player:
                continue
            if player_id in game.players:
                margins_with.append(game.margin)
                continue
            margins_without.append(game.margin)

        avg_with = sum(margins_with) / len(margins_with) if margins_with else None
        avg_without = sum(margins_without) / len(margins_without) if margins_without else None
        wowy_score = None
        if avg_with is not None and avg_without is not None:
            wowy_score = avg_with - avg_without

        results[player_id] = WowyPlayerValue(
            games_with=len(margins_with),
            games_without=len(margins_without),
            avg_margin_with=avg_with,
            avg_margin_without=avg_without,
            value=wowy_score,
            raw_value=wowy_score,
        )
        if progress is not None:
            progress(index, total_players, f"player={player_id}")

    return results


def filter_results(
    results: dict[int, WowyPlayerValue],
    *,
    min_games_with: int,
    min_games_without: int,
) -> dict[int, WowyPlayerValue]:
    filtered: dict[int, WowyPlayerValue] = {}
    for player_id, value in results.items():
        if value.games_with < min_games_with:
            continue
        if value.games_without < min_games_without:
            continue
        if value.value is None:
            continue
        filtered[player_id] = value
    return filtered
