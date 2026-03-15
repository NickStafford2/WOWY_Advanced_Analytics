from __future__ import annotations

from typing import Callable

from wowy.types import WowyGameRecord, WowyPlayerStats


ProgressFn = Callable[[int, int, str | None], None]


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
        team_seasons_with_player: set[tuple[str, str]] = set()

        for game in games:
            if player in game.players:
                team_seasons_with_player.add((game.team, game.season))

        for game in games:
            if (game.team, game.season) not in team_seasons_with_player:
                continue
            if player in game.players:
                margins_with.append(game.margin)
            else:
                margins_without.append(game.margin)

        avg_with = sum(margins_with) / len(margins_with) if margins_with else None
        avg_without = (
            sum(margins_without) / len(margins_without) if margins_without else None
        )

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
