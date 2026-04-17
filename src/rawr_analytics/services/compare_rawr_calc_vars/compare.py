from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from itertools import product

from rawr_analytics.metrics._player_context import PlayerSeasonFilters
from rawr_analytics.metrics.rawr.cache import load_rawr_input_records
from rawr_analytics.metrics.rawr.calculate.inputs import RawrEligibility, build_rawr_request
from rawr_analytics.metrics.rawr.calculate.records import build_player_season_records
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.metrics.wowy.cache import load_wowy_records
from rawr_analytics.metrics.wowy.calculate.inputs import WowyEligibility, build_wowy_season_inputs
from rawr_analytics.metrics.wowy.calculate.records import prepare_wowy_player_season_records
from rawr_analytics.services.compare_rawr_calc_vars._aggregation import (
    aggregate_rawr_training_records,
    aggregate_wowy_training_records,
    build_holdout_targets,
)
from rawr_analytics.services.compare_rawr_calc_vars._scoring import build_comparison_result
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_DEFAULT_SHRINKAGE_MODES = (
    RawrShrinkageMode.UNIFORM,
    RawrShrinkageMode.GAME_COUNT,
    RawrShrinkageMode.MINUTES,
)


@dataclass(frozen=True)
class ComparisonResult:
    model: str
    ridge_alpha: float | None
    shrinkage_mode: RawrShrinkageMode | None
    shrinkage_strength: float | None
    shrinkage_minute_scale: float | None
    players: int
    pearson: float | None
    spearman: float | None
    top_n_overlap: int


@dataclass(frozen=True)
class CompareRawrConfigsProgress:
    current: int
    total: int
    detail: str


CompareRawrConfigsEventFn = Callable[[CompareRawrConfigsProgress], None]


def compare_rawr_configs(
    *,
    train_seasons: list[Season],
    holdout_season: Season,
    aggregation: str = "mean",
    teams: list[Team] | None = None,
    rawr_ridge_values: list[float] | None = None,
    shrinkage_modes: list[str] | None = None,
    shrinkage_strength_values: list[float] | None = None,
    shrinkage_minute_scale_values: list[float] | None = None,
    rawr_min_games: int = 35,
    holdout_min_games_with: int = 15,
    holdout_min_games_without: int = 2,
    min_average_minutes: float = 30.0,
    min_total_minutes: float = 600.0,
    top_n: int = 20,
    event_fn: CompareRawrConfigsEventFn | None = None,
) -> list[ComparisonResult]:
    _validate_request(
        train_seasons=train_seasons,
        holdout_season=holdout_season,
        rawr_ridge_values=rawr_ridge_values,
        shrinkage_strength_values=shrinkage_strength_values,
        shrinkage_minute_scale_values=shrinkage_minute_scale_values,
        top_n=top_n,
    )

    total_steps = _count_evaluation_steps(
        rawr_ridge_values=rawr_ridge_values,
        shrinkage_modes=shrinkage_modes,
        shrinkage_strength_values=shrinkage_strength_values,
        shrinkage_minute_scale_values=shrinkage_minute_scale_values,
    )
    completed_steps = 0
    normalized_shrinkage_modes = _normalize_shrinkage_modes(shrinkage_modes)
    wowy_teams = teams or Team.all()
    player_filters = PlayerSeasonFilters(
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    holdout_eligibility = WowyEligibility(
        min_games_with=holdout_min_games_with,
        min_games_without=holdout_min_games_without,
    )
    rawr_eligibility = RawrEligibility(min_games=rawr_min_games)

    holdout_games, holdout_game_players = load_wowy_records(
        teams=wowy_teams,
        seasons=[holdout_season],
    )
    holdout_season_inputs = build_wowy_season_inputs(
        games=holdout_games,
        game_players=holdout_game_players,
    )
    holdout_records = prepare_wowy_player_season_records(
        season_inputs=holdout_season_inputs,
        eligibility=holdout_eligibility,
        filters=player_filters,
    )
    completed_steps += 1
    _emit_progress(
        event_fn,
        current=completed_steps,
        total=total_steps,
        detail=f"holdout {holdout_season}",
    )
    holdout_targets = build_holdout_targets(holdout_records)

    training_wowy_games, training_wowy_game_players = load_wowy_records(
        teams=wowy_teams,
        seasons=train_seasons,
    )
    training_wowy_season_inputs = build_wowy_season_inputs(
        games=training_wowy_games,
        game_players=training_wowy_game_players,
    )
    training_wowy_records = prepare_wowy_player_season_records(
        season_inputs=training_wowy_season_inputs,
        eligibility=holdout_eligibility,
        filters=player_filters,
    )
    completed_steps += 1
    _emit_progress(
        event_fn,
        current=completed_steps,
        total=total_steps,
        detail="training WOWY",
    )

    results = [
        build_comparison_result(
            result_type=ComparisonResult,
            model="wowy-baseline",
            training_scores=aggregate_wowy_training_records(
                training_wowy_records,
                aggregation=aggregation,
            ),
            holdout_targets=holdout_targets,
            top_n=top_n,
        )
    ]
    completed_steps += 1
    _emit_progress(
        event_fn,
        current=completed_steps,
        total=total_steps,
        detail="baseline scored",
    )

    for ridge_alpha, shrinkage_mode, shrinkage_strength in product(
        rawr_ridge_values or [],
        normalized_shrinkage_modes,
        shrinkage_strength_values or [],
    ):
        minute_scales = (
            shrinkage_minute_scale_values or []
            if shrinkage_mode == RawrShrinkageMode.MINUTES
            else [(shrinkage_minute_scale_values or [48.0])[0]]
        )
        for minute_scale in minute_scales:
            detail = f"alpha={ridge_alpha:.2f} mode={shrinkage_mode}"
            if shrinkage_mode == RawrShrinkageMode.MINUTES:
                detail += f" min_scale={minute_scale:.1f}"

            season_games, season_game_players = load_rawr_input_records(
                teams=teams or Team.all(),
                seasons=train_seasons,
            )
            rawr_request = build_rawr_request(
                season_games=season_games,
                season_game_players=season_game_players,
                eligibility=rawr_eligibility,
                filters=player_filters,
                ridge_alpha=ridge_alpha,
                shrinkage_mode=shrinkage_mode,
                shrinkage_strength=shrinkage_strength,
                shrinkage_minute_scale=minute_scale,
            )
            rawr_records = build_player_season_records(rawr_request)

            completed_steps += 1
            _emit_progress(
                event_fn,
                current=completed_steps,
                total=total_steps,
                detail=detail,
            )

            results.append(
                build_comparison_result(
                    result_type=ComparisonResult,
                    model="rawr",
                    training_scores=aggregate_rawr_training_records(
                        rawr_records,
                        aggregation=aggregation,
                    ),
                    holdout_targets=holdout_targets,
                    top_n=top_n,
                    ridge_alpha=ridge_alpha,
                    shrinkage_mode=shrinkage_mode,
                    shrinkage_strength=shrinkage_strength,
                    shrinkage_minute_scale=(
                        minute_scale if shrinkage_mode == RawrShrinkageMode.MINUTES else None
                    ),
                )
            )

    return sorted(
        results,
        key=lambda result: (
            result.spearman if result.spearman is not None else float("-inf"),
            result.pearson if result.pearson is not None else float("-inf"),
            result.top_n_overlap,
            result.players,
        ),
        reverse=True,
    )


def _validate_request(
    *,
    train_seasons: list[Season],
    holdout_season: Season,
    rawr_ridge_values: list[float] | None,
    shrinkage_strength_values: list[float] | None,
    shrinkage_minute_scale_values: list[float] | None,
    top_n: int,
) -> None:
    assert rawr_ridge_values is not None
    assert shrinkage_strength_values is not None
    assert shrinkage_minute_scale_values is not None

    if top_n <= 0:
        raise ValueError("top_n must be positive")

    train_season_ids = {season.year_string_nba_api for season in train_seasons}
    if holdout_season.year_string_nba_api in train_season_ids:
        raise ValueError("holdout season must not be included in training seasons")


def _emit_progress(
    event_fn: CompareRawrConfigsEventFn | None,
    *,
    current: int,
    total: int,
    detail: str,
) -> None:
    if event_fn is None:
        return
    event_fn(CompareRawrConfigsProgress(current=current, total=total, detail=detail))


def _normalize_shrinkage_modes(
    shrinkage_modes: list[str] | None,
) -> list[RawrShrinkageMode]:
    if shrinkage_modes is None:
        return list(_DEFAULT_SHRINKAGE_MODES)
    return [RawrShrinkageMode.parse(mode) for mode in shrinkage_modes]


def _count_evaluation_steps(
    *,
    rawr_ridge_values: list[float] | None,
    shrinkage_modes: list[str] | None,
    shrinkage_strength_values: list[float] | None,
    shrinkage_minute_scale_values: list[float] | None,
) -> int:
    rawr_configs = 0
    for _, shrinkage_mode, _ in product(
        rawr_ridge_values or [],
        _normalize_shrinkage_modes(shrinkage_modes),
        shrinkage_strength_values or [],
    ):
        if shrinkage_mode == RawrShrinkageMode.MINUTES:
            rawr_configs += len(shrinkage_minute_scale_values or [])
            continue
        rawr_configs += 1
    return 3 + rawr_configs


__all__ = [
    "CompareRawrConfigsEventFn",
    "CompareRawrConfigsProgress",
    "ComparisonResult",
    "compare_rawr_configs",
]
