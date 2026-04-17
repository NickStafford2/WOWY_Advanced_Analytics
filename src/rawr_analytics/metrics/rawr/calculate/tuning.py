from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from rawr_analytics.metrics.rawr.calculate._analysis import (
    RawrModel,
    build_feature_row,
    fit_regression_model,
    team_season_key,
)
from rawr_analytics.metrics.rawr.calculate._observations import (
    RawrObservation,
    count_player_season_games,
)
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class _RidgeTuningResult:
    alpha: float
    validation_mse: float


@dataclass(frozen=True)
class _RidgeTuningSummary:
    best_alpha: float
    results: list[_RidgeTuningResult]


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
        model = fit_regression_model(
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


def _predict_margin(
    observation: RawrObservation,
    model: RawrModel,
    *,
    season: Season,
) -> float:
    row = build_feature_row(
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
        team_effect_key=team_season_key(observation.home_team, season),
        opponent_effect_key=team_season_key(observation.away_team, season),
    )
    return sum(
        weight * coefficient for weight, coefficient in zip(row, model.coefficients, strict=True)
    )
