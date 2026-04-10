from __future__ import annotations

from typing import overload

DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES = 10.0


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
