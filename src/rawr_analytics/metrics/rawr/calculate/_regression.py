from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

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
class RawrModel:
    player_ids: list[int]
    team_seasons: list[_TeamSeasonKey]
    coefficients: list[float]


def fit_player_rawr(
    observations: list[RawrObservation],
    *,
    player_ids: list[int],
    season: Season,
    ridge_alpha: float = 1.0,
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM,
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    progress: _ProgressFn | None = None,
) -> dict[int, float]:
    assert ridge_alpha >= 0, f"Ridge alpha must be non-negative: {ridge_alpha}"
    assert observations, "At least one RAWR observation is required"
    assert player_ids, "At least one RAWR player is required"
    assert len(player_ids) == len(set(player_ids)), "RAWR player ids must be unique"

    model = fit_regression_model(
        observations,
        player_ids,
        season=season,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
        progress=progress,
    )
    return {player_id: model.coefficients[index + 2] for index, player_id in enumerate(player_ids)}


def fit_regression_model(
    observations: list[RawrObservation],
    player_ids: list[int],
    *,
    season: Season,
    ridge_alpha: float = 1.0,
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM,
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    progress: _ProgressFn | None = None,
) -> RawrModel:
    team_seasons = sorted(
        {team_season_key(observation.home_team, season) for observation in observations}
        | {team_season_key(observation.away_team, season) for observation in observations}
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
        home_team_season = team_season_key(observation.home_team, season)
        away_team_season = team_season_key(observation.away_team, season)
        _accumulate_row(
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
        _accumulate_row(
            gram=gram,
            target=target,
            row=build_feature_row(
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

    return RawrModel(
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


def build_feature_row(
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


def team_season_key(team: Team, season: Season) -> _TeamSeasonKey:
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
