from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np

from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.services.compare_rawr_calc_vars._aggregation import (
    AggregatedPlayerValue,
)


def build_comparison_result(
    *,
    result_type: type[Any],
    model: str,
    training_scores: Mapping[int, AggregatedPlayerValue],
    holdout_targets: Mapping[int, AggregatedPlayerValue],
    top_n: int,
    ridge_alpha: float | None = None,
    shrinkage_mode: RawrShrinkageMode | None = None,
    shrinkage_strength: float | None = None,
    shrinkage_minute_scale: float | None = None,
) -> Any:
    shared_player_ids = sorted(set(training_scores) & set(holdout_targets))
    train_values = [training_scores[player_id].value for player_id in shared_player_ids]
    holdout_values = [holdout_targets[player_id].value for player_id in shared_player_ids]

    return result_type(
        model=model,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
        players=len(shared_player_ids),
        pearson=pearson_correlation(train_values, holdout_values),
        spearman=spearman_correlation(train_values, holdout_values),
        top_n_overlap=top_n_overlap(training_scores, holdout_targets, top_n=top_n),
    )


def pearson_correlation(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(ys) < 2:
        return None
    if len(set(xs)) <= 1 or len(set(ys)) <= 1:
        return None
    return float(np.corrcoef(xs, ys)[0][1])


def spearman_correlation(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(ys) < 2:
        return None
    ranked_xs = rank_values(xs)
    ranked_ys = rank_values(ys)
    return pearson_correlation(ranked_xs, ranked_ys)


def rank_values(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    position = 0
    while position < len(indexed):
        end = position
        while end + 1 < len(indexed) and indexed[end + 1][1] == indexed[position][1]:
            end += 1
        average_rank = (position + end + 2) / 2.0
        for tie_index in range(position, end + 1):
            original_index = indexed[tie_index][0]
            ranks[original_index] = average_rank
        position = end + 1
    return ranks


def top_n_overlap(
    training_scores: Mapping[int, AggregatedPlayerValue],
    holdout_targets: Mapping[int, AggregatedPlayerValue],
    *,
    top_n: int,
) -> int:
    if top_n <= 0:
        raise ValueError("top_n must be positive")

    shared_player_ids = set(training_scores) & set(holdout_targets)
    if not shared_player_ids:
        return 0

    ranked_train = sorted(
        shared_player_ids,
        key=lambda player_id: (
            training_scores[player_id].value,
            training_scores[player_id].player_name,
        ),
        reverse=True,
    )[:top_n]
    ranked_holdout = sorted(
        shared_player_ids,
        key=lambda player_id: (
            holdout_targets[player_id].value,
            holdout_targets[player_id].player_name,
        ),
        reverse=True,
    )[:top_n]
    return len(set(ranked_train) & set(ranked_holdout))


__all__ = [
    "build_comparison_result",
    "pearson_correlation",
    "rank_values",
    "spearman_correlation",
    "top_n_overlap",
]
