from __future__ import annotations

from collections.abc import Callable
from typing import overload

from rawr_analytics.metrics.wowy.models import WowyGame, WowyPlayerValue

ProgressFn = Callable[[int, int, str | None], None]
DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES = 10.0


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
        teams_with_player = {game.team for game in games if player_id in game.players}

        for game in games:
            if game.team not in teams_with_player:
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
            wowy_score=wowy_score,
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
        if value.wowy_score is None:
            continue
        filtered[player_id] = value
    return filtered


@overload
def compute_wowy_shrinkage_score(
    *,
    games_with: int,
    games_without: int,
    wowy_score: float,
    prior_games: float = DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
) -> float: ...


@overload
def compute_wowy_shrinkage_score(
    *,
    games_with: int,
    games_without: int,
    wowy_score: None,
    prior_games: float = DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
) -> None: ...


def compute_wowy_shrinkage_score(
    *,
    games_with: int,
    games_without: int,
    wowy_score: float | None,
    prior_games: float = DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
) -> float | None:
    if wowy_score is None:
        return None
    if prior_games < 0:
        raise ValueError("prior_games must be non-negative")
    effective_games = _harmonic_mean_sample_size(games_with, games_without)
    if effective_games <= 0:
        return 0.0
    shrinkage_factor = effective_games / (effective_games + prior_games)
    return wowy_score * shrinkage_factor


def _harmonic_mean_sample_size(games_with: int, games_without: int) -> float:
    if games_with <= 0 or games_without <= 0:
        return 0.0
    return (2.0 * games_with * games_without) / (games_with + games_without)
