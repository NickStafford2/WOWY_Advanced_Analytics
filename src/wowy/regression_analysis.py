from __future__ import annotations

from wowy.regression_data import count_player_games
from wowy.regression_types import (
    RegressionObservation,
    RegressionPlayerEstimate,
    RegressionResult,
)


def fit_player_regression(
    observations: list[RegressionObservation],
    player_names: dict[int, str],
    min_games: int = 1,
    ridge_alpha: float = 1.0,
) -> RegressionResult:
    if min_games < 0:
        raise ValueError("Minimum games filter must be non-negative")
    if ridge_alpha < 0:
        raise ValueError("Ridge alpha must be non-negative")
    if not observations:
        raise ValueError("At least one regression observation is required")

    games_by_player = count_player_games(observations)
    included_players = sorted(
        player_id
        for player_id, games in games_by_player.items()
        if games >= min_games
    )
    if not included_players:
        raise ValueError("No players met the minimum games requirement")

    coefficients = solve_normal_equation(
        observations,
        included_players,
        ridge_alpha=ridge_alpha,
    )
    intercept = coefficients[0]

    estimates = [
        RegressionPlayerEstimate(
            player_id=player_id,
            player_name=player_names.get(player_id, str(player_id)),
            games=games_by_player[player_id],
            coefficient=coefficients[index + 1],
        )
        for index, player_id in enumerate(included_players)
    ]

    return RegressionResult(
        observations=len(observations),
        players=len(estimates),
        intercept=intercept,
        estimates=estimates,
    )


def solve_normal_equation(
    observations: list[RegressionObservation],
    player_ids: list[int],
    ridge_alpha: float = 1.0,
) -> list[float]:
    feature_count = len(player_ids) + 1
    player_index = {player_id: index + 1 for index, player_id in enumerate(player_ids)}

    gram = [[0.0 for _ in range(feature_count)] for _ in range(feature_count)]
    target = [0.0 for _ in range(feature_count)]

    for observation in observations:
        row = [0.0 for _ in range(feature_count)]
        row[0] = 1.0
        for player_id, weight in observation.player_weights.items():
            feature_index = player_index.get(player_id)
            if feature_index is not None:
                row[feature_index] = weight

        for i in range(feature_count):
            target[i] += row[i] * observation.margin
            for j in range(feature_count):
                gram[i][j] += row[i] * row[j]

    for diagonal_index in range(1, feature_count):
        gram[diagonal_index][diagonal_index] += ridge_alpha

    return solve_linear_system(gram, target)


def solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [row[:] + [value] for row, value in zip(matrix, vector, strict=True)]

    for pivot_index in range(size):
        pivot_row = max(
            range(pivot_index, size),
            key=lambda row_index: abs(augmented[row_index][pivot_index]),
        )
        pivot_value = augmented[pivot_row][pivot_index]
        if abs(pivot_value) < 1e-9:
            raise ValueError(
                "Regression system is singular; the current game-level player-only design matrix is not identifiable for this input."
            )

        if pivot_row != pivot_index:
            augmented[pivot_index], augmented[pivot_row] = (
                augmented[pivot_row],
                augmented[pivot_index],
            )

        pivot_value = augmented[pivot_index][pivot_index]
        for column_index in range(pivot_index, size + 1):
            augmented[pivot_index][column_index] /= pivot_value

        for row_index in range(size):
            if row_index == pivot_index:
                continue
            factor = augmented[row_index][pivot_index]
            if factor == 0:
                continue
            for column_index in range(pivot_index, size + 1):
                augmented[row_index][column_index] -= (
                    factor * augmented[pivot_index][column_index]
                )

    return [augmented[row_index][size] for row_index in range(size)]
