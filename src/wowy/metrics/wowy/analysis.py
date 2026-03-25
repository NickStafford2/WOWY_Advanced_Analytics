from __future__ import annotations

from typing import Callable, overload

from wowy.metrics.wowy.models import WowyGameRecord, WowyPlayerStats

ProgressFn = Callable[[int, int, str | None], None]
DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES = 10.0


def compute_wowy(
    games: list[WowyGameRecord],
    progress: ProgressFn | None = None,
) -> dict[int, WowyPlayerStats]:
    all_players: set[int] = set()
    for game in games:
        all_players.update(game.players)

    results: dict[int, WowyPlayerStats] = {}
    sorted_players = sorted(all_players)
    total_players = len(sorted_players)
    for index, player in enumerate(sorted_players, start=1):
        margins_with: list[float] = []
        margins_without: list[float] = []
        team_seasons_with_player: set[tuple[int | str, str]] = set()

        for game in games:
            if player in game.players:
                team_seasons_with_player.add((game.identity_team, game.season))

        for game in games:
            if (game.identity_team, game.season) not in team_seasons_with_player:
                continue
            if player in game.players:
                margins_with.append(game.margin)
            else:
                margins_without.append(game.margin)

        avg_with = sum(margins_with) / len(margins_with) if margins_with else None
        avg_without = sum(margins_without) / len(margins_without) if margins_without else None

        wowy_score: float | None = None
        if avg_with is not None and avg_without is not None:
            wowy_score = avg_with - avg_without

        results[player] = WowyPlayerStats(
            games_with=len(margins_with),
            games_without=len(margins_without),
            avg_margin_with=avg_with,
            avg_margin_without=avg_without,
            wowy_score=wowy_score,
        )

        if progress is not None:
            progress(index, total_players, f"player={player}")

    return results


def filter_results(
    results: dict[int, WowyPlayerStats],
    min_games_with: int = 1,
    min_games_without: int = 1,
) -> dict[int, WowyPlayerStats]:
    filtered: dict[int, WowyPlayerStats] = {}

    for player, stats in results.items():
        if stats.games_with < min_games_with:
            continue
        if stats.games_without < min_games_without:
            continue
        if stats.wowy_score is None:
            continue
        filtered[player] = stats

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
    effective_games = harmonic_mean_sample_size(games_with, games_without)
    if effective_games <= 0:
        return 0.0
    shrinkage_factor = effective_games / (effective_games + prior_games)
    return wowy_score * shrinkage_factor


def apply_wowy_shrinkage(
    results: dict[int, WowyPlayerStats],
    *,
    prior_games: float = DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
) -> dict[int, WowyPlayerStats]:
    return {
        player_id: WowyPlayerStats(
            games_with=stats.games_with,
            games_without=stats.games_without,
            avg_margin_with=stats.avg_margin_with,
            avg_margin_without=stats.avg_margin_without,
            wowy_score=compute_wowy_shrinkage_score(
                games_with=stats.games_with,
                games_without=stats.games_without,
                wowy_score=stats.wowy_score,
                prior_games=prior_games,
            ),
            average_minutes=stats.average_minutes,
            total_minutes=stats.total_minutes,
        )
        for player_id, stats in results.items()
    }


def harmonic_mean_sample_size(games_with: int, games_without: int) -> float:
    if games_with <= 0 or games_without <= 0:
        return 0.0
    return (2.0 * games_with * games_without) / (games_with + games_without)
