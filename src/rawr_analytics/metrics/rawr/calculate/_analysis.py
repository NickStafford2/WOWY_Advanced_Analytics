from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from statistics import mean

import numpy as np

from rawr_analytics.metrics.rawr.calculate._observations import (
    RawrObservation,
    count_player_season_games,
    count_player_season_minutes,
)
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

type _ProgressFn = Callable[[int, int, str | None], None]
type _TeamSeasonKey = tuple[int, Season]


@dataclass(frozen=True)
class _RawrModel:
    player_ids: list[int]
    team_seasons: list[_TeamSeasonKey]
    coefficients: list[float]


@dataclass(frozen=True)
class _RidgeTuningResult:
    alpha: float
    validation_mse: float


@dataclass(frozen=True)
class _RidgeTuningSummary:
    best_alpha: float
    results: list[_RidgeTuningResult]


def fit_player_rawr(
    observations: list[RawrObservation],
    *,
    season: Season,
    min_games: int = 1,
    ridge_alpha: float = 1.0,
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM,
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    progress: _ProgressFn | None = None,
) -> dict[int, float]:
    if min_games < 0:
        raise ValueError("Minimum games filter must be non-negative")
    if ridge_alpha < 0:
        raise ValueError("Ridge alpha must be non-negative")
    shrinkage_mode = RawrShrinkageMode.validate(
        shrinkage_mode,
        shrinkage_strength,
        shrinkage_minute_scale,
    )
    if not observations:
        raise ValueError("At least one RAWR observation is required")

    games_by_player = count_player_season_games(observations)
    included_player_ids = sorted(
        player_id for player_id, games in games_by_player.items() if games >= min_games
    )
    if not included_player_ids:
        raise ValueError("No players met the minimum games requirement")

    model = _fit_regression_model(
        observations,
        included_player_ids,
        season=season,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
        progress=progress,
    )
    return {
        player_id: model.coefficients[index + 2]
        for index, player_id in enumerate(included_player_ids)
    }


def _fit_regression_model(
    observations: list[RawrObservation],
    player_ids: list[int],
    *,
    season: Season,
    ridge_alpha: float = 1.0,
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM,
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    progress: _ProgressFn | None = None,
) -> _RawrModel:
    shrinkage_mode = RawrShrinkageMode.validate(
        shrinkage_mode,
        shrinkage_strength,
        shrinkage_minute_scale,
    )
    team_seasons = sorted(
        {_team_season_key(observation.home_team, season) for observation in observations}
        | {_team_season_key(observation.away_team, season) for observation in observations}
    )
    player_offset = 2
    team_effect_offset = player_offset + len(player_ids)
    opponent_effect_offset = team_effect_offset + len(team_seasons)
    feature_count = opponent_effect_offset + len(team_seasons)
    player_index = {player_id: index + 2 for index, player_id in enumerate(player_ids)}
    team_effect_index = {
        team_season: team_effect_offset + index for index, team_season in enumerate(team_seasons)
    }
    opponent_effect_index = {
        team_season: opponent_effect_offset + index
        for index, team_season in enumerate(team_seasons)
    }
    player_penalties = _build_player_penalties(
        observations=observations,
        player_ids=player_ids,
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
        home_team_season = _team_season_key(observation.home_team, season)
        away_team_season = _team_season_key(observation.away_team, season)
        _accumulate_row(
            gram=gram,
            target=target,
            row=_build_feature_row(
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
        _accumulate_row(
            gram=gram,
            target=target,
            row=_build_feature_row(
                feature_count=feature_count,
                player_index=player_index,
                team_effect_index=team_effect_index,
                opponent_effect_index=opponent_effect_index,
                player_weights={
                    player_id: -weight for player_id, weight in observation.player_weights.items()
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

    for player_id, diagonal_index in player_index.items():
        gram[diagonal_index][diagonal_index] += player_penalties[player_id]
        completed_steps += 1
        if progress is not None:
            progress(completed_steps, total_steps, "applying ridge penalty")

    for diagonal_index in range(team_effect_offset, feature_count):
        gram[diagonal_index][diagonal_index] += ridge_alpha
        completed_steps += 1
        if progress is not None:
            progress(completed_steps, total_steps, "applying ridge penalty")

    return _RawrModel(
        player_ids=player_ids,
        team_seasons=team_seasons,
        coefficients=_solve_linear_system(
            gram,
            target,
            progress=progress,
            progress_offset=completed_steps,
            progress_total=total_steps,
        ),
    )


def _predict_margin(
    observation: RawrObservation,
    model: _RawrModel,
    *,
    season: Season,
) -> float:
    row = _build_feature_row(
        feature_count=len(model.coefficients),
        player_index={player_id: index + 2 for index, player_id in enumerate(model.player_ids)},
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
        team_effect_key=_team_season_key(observation.home_team, season),
        opponent_effect_key=_team_season_key(observation.away_team, season),
    )
    return sum(
        weight * coefficient for weight, coefficient in zip(row, model.coefficients, strict=True)
    )


# todo: use this later. I used it a while ago. still need it. Don't delete.
def tune_ridge_alpha(
    observations: list[RawrObservation],
    player_names: dict[int, str],
    *,
    season: Season,
    alphas: list[float],
    min_games: int = 1,
    validation_fraction: float = 0.2,
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM,
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
) -> _RidgeTuningSummary:
    if not observations:
        raise ValueError("At least one RAWR observation is required")
    if not alphas:
        raise ValueError("At least one ridge alpha is required")
    if not 0.0 < validation_fraction < 0.5:
        raise ValueError("Validation fraction must be between 0 and 0.5")
    shrinkage_mode = RawrShrinkageMode.validate(
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
    included_player_ids = sorted(
        player_id
        for player_id, games in training_player_counts.items()
        if games >= min_games and player_id in player_names
    )
    if not included_player_ids:
        raise ValueError("No players met the minimum games requirement in training data")

    results: list[_RidgeTuningResult] = []
    for alpha in alphas:
        model = _fit_regression_model(
            training,
            included_player_ids,
            season=season,
            ridge_alpha=alpha,
            shrinkage_mode=shrinkage_mode,
            shrinkage_strength=shrinkage_strength,
            shrinkage_minute_scale=shrinkage_minute_scale,
        )
        validation_mse = mean(
            (_predict_margin(observation, model, season=season) - observation.margin) ** 2
            for observation in validation
        )
        results.append(_RidgeTuningResult(alpha=alpha, validation_mse=validation_mse))

    best = min(results, key=lambda result: result.validation_mse)
    return _RidgeTuningSummary(best_alpha=best.alpha, results=results)


def _build_feature_row(
    feature_count: int,
    player_index: dict[int, int],
    team_effect_index: dict[_TeamSeasonKey, int],
    opponent_effect_index: dict[_TeamSeasonKey, int],
    player_weights: dict[int, float],
    home_court_sign: float,
    team_effect_key: _TeamSeasonKey,
    opponent_effect_key: _TeamSeasonKey,
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


def _build_player_penalties(
    *,
    observations: list[RawrObservation],
    player_ids: list[int],
    ridge_alpha: float,
    shrinkage_mode: RawrShrinkageMode,
    shrinkage_strength: float,
    shrinkage_minute_scale: float = 48.0,
) -> dict[int, float]:
    if shrinkage_mode == RawrShrinkageMode.UNIFORM:
        return dict.fromkeys(player_ids, ridge_alpha)

    if shrinkage_mode == RawrShrinkageMode.GAME_COUNT:
        games_by_player = count_player_season_games(observations)
        penalties: dict[int, float] = {}
        for player_id in player_ids:
            games = games_by_player[player_id]
            penalties[player_id] = ridge_alpha / (games**shrinkage_strength)
        return penalties

    minutes_by_player = count_player_season_minutes(observations)
    penalties: dict[int, float] = {}
    for player_id in player_ids:
        if player_id not in minutes_by_player:
            raise ValueError(
                "Minute-aware shrinkage requires player minute totals for every included player"
            )
        scaled_minutes = minutes_by_player[player_id] / shrinkage_minute_scale
        penalties[player_id] = ridge_alpha / (scaled_minutes**shrinkage_strength)
    return penalties


def _accumulate_row(
    gram: np.ndarray,
    target: np.ndarray,
    row: np.ndarray,
    margin: float,
) -> None:
    target += row * margin
    gram += np.outer(row, row)


def _team_season_key(team: Team, season: Season) -> _TeamSeasonKey:
    return team.team_id, season


def _solve_linear_system(
    matrix: np.ndarray,
    vector: np.ndarray,
    progress: _ProgressFn | None = None,
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
            "RAWR system is singular; the current game-level rawr design "
            "matrix is not identifiable for this input."
        ) from exc

    return solution.tolist()
