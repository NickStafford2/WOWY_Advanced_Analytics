from __future__ import annotations

from statistics import mean
from typing import Callable

import numpy as np

from wowy.apps.rawr.data import count_player_season_games
from wowy.apps.rawr.models import (
    RawrModel,
    RawrObservation,
    RawrPlayerEstimate,
    RawrResult,
    RidgeTuningResult,
    RidgeTuningSummary,
)

ProgressFn = Callable[[int, int, str | None], None]
ShrinkageMode = str


def fit_player_rawr(
    observations: list[RawrObservation],
    player_names: dict[int, str],
    min_games: int = 1,
    ridge_alpha: float = 1.0,
    shrinkage_mode: ShrinkageMode = "uniform",
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    progress: ProgressFn | None = None,
) -> RawrResult:
    if min_games < 0:
        raise ValueError("Minimum games filter must be non-negative")
    if ridge_alpha < 0:
        raise ValueError("Ridge alpha must be non-negative")
    validate_shrinkage_settings(
        shrinkage_mode,
        shrinkage_strength,
        shrinkage_minute_scale,
    )
    if not observations:
        raise ValueError("At least one RAWR observation is required")

    games_by_player_season = count_player_season_games(observations)
    included_player_keys = sorted(
        player_key
        for player_key, games in games_by_player_season.items()
        if games >= min_games
    )
    if not included_player_keys:
        raise ValueError("No players met the minimum games requirement")

    model = fit_regression_model(
        observations,
        included_player_keys,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
        progress=progress,
    )
    intercept = model.coefficients[0]
    home_court_advantage = model.coefficients[1]

    estimates = [
        RawrPlayerEstimate(
            season=season,
            player_id=player_id,
            player_name=player_names.get(player_id, str(player_id)),
            games=games_by_player_season[(season, player_id)],
            average_minutes=None,
            total_minutes=None,
            coefficient=model.coefficients[index + 2],
        )
        for index, (season, player_id) in enumerate(included_player_keys)
    ]

    return RawrResult(
        observations=len(observations),
        players=len(estimates),
        intercept=intercept,
        home_court_advantage=home_court_advantage,
        estimates=estimates,
    )


def fit_regression_model(
    observations: list[RawrObservation],
    player_keys: list[tuple[str, int]],
    ridge_alpha: float = 1.0,
    shrinkage_mode: ShrinkageMode = "uniform",
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    progress: ProgressFn | None = None,
) -> RawrModel:
    validate_shrinkage_settings(
        shrinkage_mode,
        shrinkage_strength,
        shrinkage_minute_scale,
    )
    team_seasons = sorted(
        {
            team_season_key(
                observation.home_team_id or observation.home_team,
                observation.season,
            )
            for observation in observations
        }
        | {
            team_season_key(
                observation.away_team_id or observation.away_team,
                observation.season,
            )
            for observation in observations
        }
    )
    player_offset = 2
    team_effect_offset = player_offset + len(player_keys)
    opponent_effect_offset = team_effect_offset + len(team_seasons)
    feature_count = opponent_effect_offset + len(team_seasons)
    player_index = {
        player_key: index + 2 for index, player_key in enumerate(player_keys)
    }
    team_effect_index = {
        team_season: team_effect_offset + index
        for index, team_season in enumerate(team_seasons)
    }
    opponent_effect_index = {
        team_season: opponent_effect_offset + index
        for index, team_season in enumerate(team_seasons)
    }
    player_penalties = build_player_penalties(
        observations=observations,
        player_keys=player_keys,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
    )

    gram = np.zeros((feature_count, feature_count), dtype=float)
    target = np.zeros(feature_count, dtype=float)

    total_steps = (len(observations) * 2) + max(feature_count - 2, 0) + feature_count
    completed_steps = 0

    for observation in observations:
        home_team_season = team_season_key(
            observation.home_team_id or observation.home_team,
            observation.season,
        )
        away_team_season = team_season_key(
            observation.away_team_id or observation.away_team,
            observation.season,
        )
        accumulate_row(
            gram=gram,
            target=target,
            row=build_feature_row(
                feature_count=feature_count,
                player_index=player_index,
                team_effect_index=team_effect_index,
                opponent_effect_index=opponent_effect_index,
                season=observation.season,
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
                season=observation.season,
                player_weights={
                    player_id: -weight
                    for player_id, weight in observation.player_weights.items()
                },
                home_court_sign=-1.0,
                team_effect_key=away_team_season,
                opponent_effect_key=home_team_season,
            ),
            margin=-observation.margin,
        )
        completed_steps += 1
        if progress is not None:
            progress(completed_steps, total_steps, "building gram matrix")

    for player_key, diagonal_index in player_index.items():
        gram[diagonal_index][diagonal_index] += player_penalties[player_key]
        completed_steps += 1
        if progress is not None:
            progress(completed_steps, total_steps, "applying ridge penalty")

    for diagonal_index in range(team_effect_offset, feature_count):
        gram[diagonal_index][diagonal_index] += ridge_alpha
        completed_steps += 1
        if progress is not None:
            progress(completed_steps, total_steps, "applying ridge penalty")

    return RawrModel(
        player_keys=player_keys,
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
    observation: RawrObservation,
    model: RawrModel,
) -> float:
    row = build_feature_row(
        feature_count=len(model.coefficients),
        player_index={
            player_key: index + 2 for index, player_key in enumerate(model.player_keys)
        },
        team_effect_index={
            team_season: len(model.player_keys) + 2 + index
            for index, team_season in enumerate(model.team_seasons)
        },
        opponent_effect_index={
            team_season: len(model.player_keys) + 2 + len(model.team_seasons) + index
            for index, team_season in enumerate(model.team_seasons)
        },
        season=observation.season,
        player_weights=observation.player_weights,
        home_court_sign=1.0,
        team_effect_key=team_season_key(
            observation.home_team_id or observation.home_team,
            observation.season,
        ),
        opponent_effect_key=team_season_key(
            observation.away_team_id or observation.away_team,
            observation.season,
        ),
    )
    return sum(
        weight * coefficient
        for weight, coefficient in zip(row, model.coefficients, strict=True)
    )


def tune_ridge_alpha(
    observations: list[RawrObservation],
    player_names: dict[int, str],
    alphas: list[float],
    min_games: int = 1,
    validation_fraction: float = 0.2,
    shrinkage_mode: ShrinkageMode = "uniform",
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
) -> RidgeTuningSummary:
    if not observations:
        raise ValueError("At least one RAWR observation is required")
    if not alphas:
        raise ValueError("At least one ridge alpha is required")
    if not 0.0 < validation_fraction < 0.5:
        raise ValueError("Validation fraction must be between 0 and 0.5")
    validate_shrinkage_settings(
        shrinkage_mode,
        shrinkage_strength,
        shrinkage_minute_scale,
    )

    ordered_observations = sorted(
        observations,
        key=lambda observation: (observation.game_date, observation.game_id),
    )
    validation_count = max(1, int(len(ordered_observations) * validation_fraction))
    if validation_count >= len(ordered_observations):
        raise ValueError("Validation split leaves no training observations")

    training = ordered_observations[:-validation_count]
    validation = ordered_observations[-validation_count:]
    training_player_counts = count_player_season_games(training)
    included_player_keys = sorted(
        player_key
        for player_key, games in training_player_counts.items()
        if games >= min_games and player_key[1] in player_names
    )
    if not included_player_keys:
        raise ValueError(
            "No players met the minimum games requirement in training data"
        )

    results: list[RidgeTuningResult] = []
    for alpha in alphas:
        model = fit_regression_model(
            training,
            included_player_keys,
            ridge_alpha=alpha,
            shrinkage_mode=shrinkage_mode,
            shrinkage_strength=shrinkage_strength,
            shrinkage_minute_scale=shrinkage_minute_scale,
        )
        validation_mse = mean(
            (predict_margin(observation, model) - observation.margin) ** 2
            for observation in validation
        )
        results.append(RidgeTuningResult(alpha=alpha, validation_mse=validation_mse))

    best = min(results, key=lambda result: result.validation_mse)
    return RidgeTuningSummary(best_alpha=best.alpha, results=results)


def build_feature_row(
    feature_count: int,
    player_index: dict[tuple[str, int], int],
    team_effect_index: dict[str, int],
    opponent_effect_index: dict[str, int],
    season: str,
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
        feature_index = player_index.get((season, player_id))
        if feature_index is not None:
            row[feature_index] = weight
    return row


def validate_shrinkage_settings(
    shrinkage_mode: ShrinkageMode,
    shrinkage_strength: float,
    shrinkage_minute_scale: float,
) -> None:
    if shrinkage_mode not in {"uniform", "game-count", "minutes"}:
        raise ValueError("Shrinkage mode must be 'uniform', 'game-count', or 'minutes'")
    if shrinkage_strength < 0:
        raise ValueError("Shrinkage strength must be non-negative")
    if shrinkage_minute_scale <= 0:
        raise ValueError("Shrinkage minute scale must be positive")


def build_player_penalties(
    *,
    observations: list[RawrObservation],
    player_keys: list[tuple[str, int]],
    ridge_alpha: float,
    shrinkage_mode: ShrinkageMode,
    shrinkage_strength: float,
    shrinkage_minute_scale: float = 48.0,
) -> dict[tuple[str, int], float]:
    if shrinkage_mode == "uniform":
        return {player_key: ridge_alpha for player_key in player_keys}

    if shrinkage_mode == "game-count":
        games_by_player_season = count_player_season_games(observations)
        penalties: dict[tuple[str, int], float] = {}
        for player_key in player_keys:
            games = games_by_player_season[player_key]
            penalties[player_key] = ridge_alpha / (games**shrinkage_strength)
        return penalties

    minutes_by_player_season = count_player_season_minutes(observations)
    penalties: dict[tuple[str, int], float] = {}
    for player_key in player_keys:
        if player_key not in minutes_by_player_season:
            raise ValueError(
                "Minute-aware shrinkage requires player minute totals for every included player-season"
            )
        scaled_minutes = minutes_by_player_season[player_key] / shrinkage_minute_scale
        penalties[player_key] = ridge_alpha / (scaled_minutes**shrinkage_strength)
    return penalties


def count_player_season_minutes(
    observations: list[RawrObservation],
) -> dict[tuple[str, int], float]:
    minutes_by_player_season: dict[tuple[str, int], float] = {}
    for observation in observations:
        if observation.player_minutes is None:
            continue
        for player_id, minutes in observation.player_minutes.items():
            key = (observation.season, player_id)
            minutes_by_player_season[key] = (
                minutes_by_player_season.get(key, 0.0) + minutes
            )
    return minutes_by_player_season


def accumulate_row(
    gram: np.ndarray,
    target: np.ndarray,
    row: np.ndarray,
    margin: float,
) -> None:
    target += row * margin
    gram += np.outer(row, row)


def team_season_key(team: int | str, season: str) -> str:
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
            "RAWR system is singular; the current game-level rawr design matrix is not identifiable for this input."
        ) from exc

    return solution.tolist()
