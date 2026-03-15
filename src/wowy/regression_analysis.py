from __future__ import annotations

from statistics import mean
from typing import Callable

import numpy as np

from wowy.regression_data import count_player_games
from wowy.regression_types import (
    RegressionModel,
    RegressionObservation,
    RegressionPlayerEstimate,
    RegressionResult,
    RidgeTuningResult,
    RidgeTuningSummary,
)

ProgressFn = Callable[[int, int, str | None], None]


def fit_player_regression(
    observations: list[RegressionObservation],
    player_names: dict[int, str],
    min_games: int = 1,
    ridge_alpha: float = 1.0,
    progress: ProgressFn | None = None,
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

    model = fit_regression_model(
        observations,
        included_players,
        ridge_alpha=ridge_alpha,
        progress=progress,
    )
    intercept = model.coefficients[0]
    home_court_advantage = model.coefficients[1]

    estimates = [
        RegressionPlayerEstimate(
            player_id=player_id,
            player_name=player_names.get(player_id, str(player_id)),
            games=games_by_player[player_id],
            average_minutes=None,
            total_minutes=None,
            coefficient=model.coefficients[index + 2],
        )
        for index, player_id in enumerate(included_players)
    ]

    return RegressionResult(
        observations=len(observations),
        players=len(estimates),
        intercept=intercept,
        home_court_advantage=home_court_advantage,
        estimates=estimates,
    )

def fit_regression_model(
    observations: list[RegressionObservation],
    player_ids: list[int],
    ridge_alpha: float = 1.0,
    progress: ProgressFn | None = None,
) -> RegressionModel:
    team_seasons = sorted(
        {
            team_season_key(observation.home_team, observation.season)
            for observation in observations
        }
        | {
            team_season_key(observation.away_team, observation.season)
            for observation in observations
        }
    )
    player_offset = 2
    team_effect_offset = player_offset + len(player_ids)
    opponent_effect_offset = team_effect_offset + len(team_seasons)
    feature_count = opponent_effect_offset + len(team_seasons)
    player_index = {player_id: index + 2 for index, player_id in enumerate(player_ids)}
    team_effect_index = {
        team_season: team_effect_offset + index
        for index, team_season in enumerate(team_seasons)
    }
    opponent_effect_index = {
        team_season: opponent_effect_offset + index
        for index, team_season in enumerate(team_seasons)
    }

    gram = np.zeros((feature_count, feature_count), dtype=float)
    target = np.zeros(feature_count, dtype=float)

    total_steps = (len(observations) * 2) + max(feature_count - 2, 0) + feature_count
    completed_steps = 0

    for observation in observations:
        home_team_season = team_season_key(observation.home_team, observation.season)
        away_team_season = team_season_key(observation.away_team, observation.season)
        accumulate_row(
            gram=gram,
            target=target,
            row=build_feature_row(
                feature_count=feature_count,
                player_index=player_index,
                team_effect_index=team_effect_index,
                opponent_effect_index=opponent_effect_index,
                player_weights=observation.player_weights,
                home_court_sign=1.0,
                team_effect_key=home_team_season,
                opponent_effect_key=away_team_season,
            ),
            margin=observation.margin,
        )
        completed_steps += 1
        if progress is not None:
            progress(completed_steps, total_steps, "building gram matrix")
        accumulate_row(
            gram=gram,
            target=target,
            row=build_feature_row(
                feature_count=feature_count,
                player_index=player_index,
                team_effect_index=team_effect_index,
                opponent_effect_index=opponent_effect_index,
                player_weights={player_id: -weight for player_id, weight in observation.player_weights.items()},
                home_court_sign=-1.0,
                team_effect_key=away_team_season,
                opponent_effect_key=home_team_season,
            ),
            margin=-observation.margin,
        )
        completed_steps += 1
        if progress is not None:
            progress(completed_steps, total_steps, "building gram matrix")

    for diagonal_index in range(2, feature_count):
        gram[diagonal_index][diagonal_index] += ridge_alpha
        completed_steps += 1
        if progress is not None:
            progress(completed_steps, total_steps, "applying ridge penalty")

    return RegressionModel(
        player_ids=player_ids,
        team_seasons=team_seasons,
        coefficients=solve_linear_system(
            gram,
            target,
            progress=progress,
            progress_offset=completed_steps,
            progress_total=total_steps,
        ),
    )


def predict_margin(
    observation: RegressionObservation,
    model: RegressionModel,
) -> float:
    row = build_feature_row(
        feature_count=len(model.coefficients),
        player_index={
            player_id: index + 2 for index, player_id in enumerate(model.player_ids)
        },
        team_effect_index={
            team_season: len(model.player_ids) + 2 + index
            for index, team_season in enumerate(model.team_seasons)
        },
        opponent_effect_index={
            team_season: len(model.player_ids) + 2 + len(model.team_seasons) + index
            for index, team_season in enumerate(model.team_seasons)
        },
        player_weights=observation.player_weights,
        home_court_sign=1.0,
        team_effect_key=team_season_key(observation.home_team, observation.season),
        opponent_effect_key=team_season_key(observation.away_team, observation.season),
    )
    return sum(weight * coefficient for weight, coefficient in zip(row, model.coefficients, strict=True))


def tune_ridge_alpha(
    observations: list[RegressionObservation],
    player_names: dict[int, str],
    alphas: list[float],
    min_games: int = 1,
    validation_fraction: float = 0.2,
) -> RidgeTuningSummary:
    if not observations:
        raise ValueError("At least one regression observation is required")
    if not alphas:
        raise ValueError("At least one ridge alpha is required")
    if not 0.0 < validation_fraction < 0.5:
        raise ValueError("Validation fraction must be between 0 and 0.5")

    ordered_observations = sorted(
        observations,
        key=lambda observation: (observation.game_date, observation.game_id),
    )
    validation_count = max(1, int(len(ordered_observations) * validation_fraction))
    if validation_count >= len(ordered_observations):
        raise ValueError("Validation split leaves no training observations")

    training = ordered_observations[:-validation_count]
    validation = ordered_observations[-validation_count:]
    training_player_counts = count_player_games(training)
    included_players = sorted(
        player_id
        for player_id, games in training_player_counts.items()
        if games >= min_games
        and player_id in player_names
    )
    if not included_players:
        raise ValueError("No players met the minimum games requirement in training data")

    results: list[RidgeTuningResult] = []
    for alpha in alphas:
        model = fit_regression_model(training, included_players, ridge_alpha=alpha)
        validation_mse = mean(
            (predict_margin(observation, model) - observation.margin) ** 2
            for observation in validation
        )
        results.append(RidgeTuningResult(alpha=alpha, validation_mse=validation_mse))

    best = min(results, key=lambda result: result.validation_mse)
    return RidgeTuningSummary(best_alpha=best.alpha, results=results)


def build_feature_row(
    feature_count: int,
    player_index: dict[int, int],
    team_effect_index: dict[str, int],
    opponent_effect_index: dict[str, int],
    player_weights: dict[int, float],
    home_court_sign: float,
    team_effect_key: str,
    opponent_effect_key: str,
) -> np.ndarray:
    row = np.zeros(feature_count, dtype=float)
    row[0] = 1.0
    row[1] = home_court_sign
    row[team_effect_index[team_effect_key]] = 1.0
    row[opponent_effect_index[opponent_effect_key]] = 1.0
    for player_id, weight in player_weights.items():
        feature_index = player_index.get(player_id)
        if feature_index is not None:
            row[feature_index] = weight
    return row


def accumulate_row(
    gram: np.ndarray,
    target: np.ndarray,
    row: np.ndarray,
    margin: float,
) -> None:
    target += row * margin
    gram += np.outer(row, row)


def team_season_key(team: str, season: str) -> str:
    return f"{team}:{season}"


def solve_linear_system(
    matrix: np.ndarray,
    vector: np.ndarray,
    progress: ProgressFn | None = None,
    progress_offset: int = 0,
    progress_total: int | None = None,
) -> list[float]:
    size = len(vector)
    total = progress_total if progress_total is not None else progress_offset + size

    if progress is not None:
        for pivot_index in range(size):
            progress(progress_offset + pivot_index + 1, total, "solving linear system")

    try:
        solution = np.linalg.solve(matrix, vector)
    except np.linalg.LinAlgError as exc:
        raise ValueError(
            "Regression system is singular; the current game-level regression design matrix is not identifiable for this input."
        ) from exc

    return solution.tolist()
