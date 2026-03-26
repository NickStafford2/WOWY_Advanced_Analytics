from __future__ import annotations

import argparse
from dataclasses import dataclass
from itertools import product
from pathlib import Path

import numpy as np

from rawr_analytics.data.player_metrics_db.constants import DEFAULT_PLAYER_METRICS_DB_PATH
from rawr_analytics.metrics.rawr.models import RawrPlayerSeasonRecord
from rawr_analytics.metrics.rawr.records import prepare_rawr_player_season_records
from rawr_analytics.metrics.wowy.models import WowyPlayerSeasonRecord
from rawr_analytics.metrics.wowy.records import prepare_wowy_player_season_records
from rawr_analytics.nba.source.cache import DEFAULT_SOURCE_DATA_DIR
from rawr_analytics.progress import TerminalProgressBar, print_status_box


@dataclass(frozen=True)
class AggregatedPlayerValue:
    player_id: int
    player_name: str
    value: float
    season_count: int


@dataclass(frozen=True)
class ComparisonResult:
    model: str
    ridge_alpha: float | None
    shrinkage_mode: str | None
    shrinkage_strength: float | None
    shrinkage_minute_scale: float | None
    players: int
    pearson: float | None
    spearman: float | None
    top_n_overlap: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare WOWY and RAWR training configurations against a holdout-season WOWY target."
        )
    )
    parser.add_argument(
        "--train-season",
        action="append",
        required=True,
        help="Training season. Repeat to include multiple seasons.",
    )
    parser.add_argument(
        "--holdout-season",
        required=True,
        help="Unseen season used only for evaluation.",
    )
    parser.add_argument(
        "--team",
        action="append",
        default=None,
        help="Optional team abbreviation filter. Repeat to include multiple teams.",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="Season type used when preparing inputs.",
    )
    parser.add_argument(
        "--aggregation",
        choices=["mean", "max", "latest"],
        default="mean",
        help="How to collapse multiple training seasons into one player score.",
    )
    parser.add_argument(
        "--rawr-ridge-grid",
        default="1,3,10,30",
        help="Comma-separated ridge alpha values to evaluate.",
    )
    parser.add_argument(
        "--shrinkage-mode",
        action="append",
        choices=["uniform", "game-count", "minutes"],
        default=None,
        help="RAWR shrinkage mode to evaluate. Repeat to include multiple modes.",
    )
    parser.add_argument(
        "--shrinkage-strength-grid",
        default="0,0.5,1.0",
        help="Comma-separated shrinkage strengths to evaluate.",
    )
    parser.add_argument(
        "--shrinkage-minute-scale-grid",
        default="48,240",
        help="Comma-separated minute scales to evaluate for minute-aware shrinkage.",
    )
    parser.add_argument(
        "--rawr-min-games",
        type=int,
        default=35,
        help="Minimum games required for a RAWR player-season record.",
    )
    parser.add_argument(
        "--holdout-min-games-with",
        type=int,
        default=15,
        help="Minimum games with player for holdout WOWY target rows.",
    )
    parser.add_argument(
        "--holdout-min-games-without",
        type=int,
        default=2,
        help="Minimum games without player for holdout WOWY target rows.",
    )
    parser.add_argument(
        "--min-average-minutes",
        type=float,
        default=30.0,
        help="Minimum average minutes filter applied to both training and holdout rows.",
    )
    parser.add_argument(
        "--min-total-minutes",
        type=float,
        default=600.0,
        help="Minimum total minutes filter applied to both training and holdout rows.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Top-N cutoff used for overlap reporting.",
    )
    parser.add_argument(
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help=argparse.SUPPRESS,
    )
    return parser


def parse_float_grid(raw_value: str) -> list[float]:
    values = [part.strip() for part in raw_value.split(",")]
    if not values or any(not value for value in values):
        raise ValueError("Grid values must contain one or more comma-separated numbers")
    parsed = [float(value) for value in values]
    if any(value < 0.0 for value in parsed):
        raise ValueError("Grid values must be non-negative")
    return parsed


def aggregate_wowy_training_records(
    records: list[WowyPlayerSeasonRecord],
    aggregation: str,
) -> dict[int, AggregatedPlayerValue]:
    grouped: dict[int, list[WowyPlayerSeasonRecord]] = {}
    for record in records:
        grouped.setdefault(record.player_id, []).append(record)
    return {
        player_id: AggregatedPlayerValue(
            player_id=player_id,
            player_name=player_records[0].player_name,
            value=aggregate_values(
                [record.wowy_score for record in player_records],
                [record.season for record in player_records],
                aggregation,
            ),
            season_count=len(player_records),
        )
        for player_id, player_records in grouped.items()
    }


def aggregate_rawr_training_records(
    records: list[RawrPlayerSeasonRecord],
    aggregation: str,
) -> dict[int, AggregatedPlayerValue]:
    grouped: dict[int, list[RawrPlayerSeasonRecord]] = {}
    for record in records:
        grouped.setdefault(record.player_id, []).append(record)
    return {
        player_id: AggregatedPlayerValue(
            player_id=player_id,
            player_name=player_records[0].player_name,
            value=aggregate_values(
                [record.coefficient for record in player_records],
                [record.season for record in player_records],
                aggregation,
            ),
            season_count=len(player_records),
        )
        for player_id, player_records in grouped.items()
    }


def aggregate_values(
    values: list[float],
    seasons: list[str],
    aggregation: str,
) -> float:
    if aggregation == "mean":
        return sum(values) / len(values)
    if aggregation == "max":
        return max(values)
    if aggregation == "latest":
        latest_index = max(range(len(seasons)), key=lambda index: seasons[index])
        return values[latest_index]
    raise ValueError(f"Unsupported aggregation: {aggregation}")


def build_holdout_targets(
    records: list[WowyPlayerSeasonRecord],
) -> dict[int, AggregatedPlayerValue]:
    return {
        record.player_id: AggregatedPlayerValue(
            player_id=record.player_id,
            player_name=record.player_name,
            value=record.wowy_score,
            season_count=1,
        )
        for record in records
    }


def build_comparison_result(
    *,
    model: str,
    training_scores: dict[int, AggregatedPlayerValue],
    holdout_targets: dict[int, AggregatedPlayerValue],
    top_n: int,
    ridge_alpha: float | None = None,
    shrinkage_mode: str | None = None,
    shrinkage_strength: float | None = None,
    shrinkage_minute_scale: float | None = None,
) -> ComparisonResult:
    shared_player_ids = sorted(set(training_scores) & set(holdout_targets))
    train_values = [training_scores[player_id].value for player_id in shared_player_ids]
    holdout_values = [holdout_targets[player_id].value for player_id in shared_player_ids]

    return ComparisonResult(
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
    training_scores: dict[int, AggregatedPlayerValue],
    holdout_targets: dict[int, AggregatedPlayerValue],
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


def evaluate_configs(args) -> list[ComparisonResult]:
    total_steps = count_evaluation_steps(args)
    progress_bar = TerminalProgressBar("RAWR tune", total=total_steps)
    completed_steps = 0

    holdout_records = prepare_wowy_player_season_records(
        teams=args.team,
        seasons=[args.holdout_season],
        season_type=args.season_type,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
        min_games_with=args.holdout_min_games_with,
        min_games_without=args.holdout_min_games_without,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    completed_steps += 1
    progress_bar.update(completed_steps, detail=f"holdout {args.holdout_season}")
    holdout_targets = build_holdout_targets(holdout_records)

    training_wowy_records = prepare_wowy_player_season_records(
        teams=args.team,
        seasons=args.train_season,
        season_type=args.season_type,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
        min_games_with=args.holdout_min_games_with,
        min_games_without=args.holdout_min_games_without,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    completed_steps += 1
    progress_bar.update(completed_steps, detail="training WOWY")
    results = [
        build_comparison_result(
            model="wowy-baseline",
            training_scores=aggregate_wowy_training_records(
                training_wowy_records,
                aggregation=args.aggregation,
            ),
            holdout_targets=holdout_targets,
            top_n=args.top_n,
        )
    ]
    completed_steps += 1
    progress_bar.update(completed_steps, detail="baseline scored")

    shrinkage_modes = args.shrinkage_mode or ["uniform", "game-count", "minutes"]
    for ridge_alpha, shrinkage_mode, shrinkage_strength in product(
        args.rawr_ridge_values,
        shrinkage_modes,
        args.shrinkage_strength_values,
    ):
        ridge_alpha = float(ridge_alpha)
        shrinkage_strength = float(shrinkage_strength)
        minute_scales = (
            args.shrinkage_minute_scale_values
            if shrinkage_mode == "minutes"
            else [args.shrinkage_minute_scale_values[0]]
        )
        for minute_scale in minute_scales:
            minute_scale = float(minute_scale)
            detail = f"alpha={ridge_alpha:.2f} mode={shrinkage_mode}" + (
                f" min_scale={minute_scale:.1f}" if shrinkage_mode == "minutes" else ""
            )
            rawr_records = prepare_rawr_player_season_records(
                teams=args.team,
                seasons=args.train_season,
                season_type=args.season_type,
                player_metrics_db_path=getattr(
                    args,
                    "player_metrics_db_path",
                    DEFAULT_PLAYER_METRICS_DB_PATH,
                ),
                min_games=args.rawr_min_games,
                ridge_alpha=ridge_alpha,
                shrinkage_mode=shrinkage_mode,
                shrinkage_strength=shrinkage_strength,
                shrinkage_minute_scale=minute_scale,
                min_average_minutes=args.min_average_minutes,
                min_total_minutes=args.min_total_minutes,
            )
            completed_steps += 1
            progress_bar.update(completed_steps, detail=detail)
            results.append(
                build_comparison_result(
                    model="rawr",
                    training_scores=aggregate_rawr_training_records(
                        rawr_records,
                        aggregation=args.aggregation,
                    ),
                    holdout_targets=holdout_targets,
                    top_n=args.top_n,
                    ridge_alpha=ridge_alpha,
                    shrinkage_mode=shrinkage_mode,
                    shrinkage_strength=shrinkage_strength,
                    shrinkage_minute_scale=(minute_scale if shrinkage_mode == "minutes" else None),
                )
            )

    progress_bar.finish(detail="done")

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


def count_evaluation_steps(args) -> int:
    shrinkage_modes = args.shrinkage_mode or ["uniform", "game-count", "minutes"]
    rawr_configs = 0
    for _, shrinkage_mode, _ in product(
        args.rawr_ridge_values,
        shrinkage_modes,
        args.shrinkage_strength_values,
    ):
        if shrinkage_mode == "minutes":
            rawr_configs += len(args.shrinkage_minute_scale_values)
        else:
            rawr_configs += 1
    return 3 + rawr_configs


def format_results_table(results: list[ComparisonResult]) -> str:
    if not results:
        return "No comparison rows were generated."

    lines = [
        "RAWR tuning comparison",
        "-" * 96,
        (
            f"{'model':<14} {'alpha':>7} {'mode':<10} {'strength':>9} "
            f"{'min_scale':>10} {'players':>7} {'pearson':>9} "
            f"{'spearman':>9} {'top_n':>7}"
        ),
        "-" * 96,
    ]
    for result in results:
        lines.append(
            f"{result.model:<14} "
            f"{format_float(result.ridge_alpha, decimals=2):>7} "
            f"{format_text(result.shrinkage_mode):<10} "
            f"{format_float(result.shrinkage_strength, decimals=2):>9} "
            f"{format_float(result.shrinkage_minute_scale, decimals=1):>10} "
            f"{result.players:>7} "
            f"{format_float(result.pearson, decimals=3):>9} "
            f"{format_float(result.spearman, decimals=3):>9} "
            f"{result.top_n_overlap:>7}"
        )
    return "\n".join(lines)


def format_float(value: float | None, *, decimals: int) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


def format_text(value: str | None) -> str:
    return value if value is not None else "-"


def build_summary(args, results: list[ComparisonResult]) -> str:
    train_label = ",".join(args.train_season)
    best = results[0] if results else None
    lines = [
        (
            f"train_seasons={train_label} holdout_season={args.holdout_season} "
            f"aggregation={args.aggregation} top_n={args.top_n}"
        ),
        (
            f"team_filter={','.join(args.team) if args.team else 'all-teams'} "
            f"season_type={args.season_type}"
        ),
    ]
    if best is not None:
        best_suffix = ""
        if best.model != "wowy-baseline":
            minute_scale = (
                f"{best.shrinkage_minute_scale}" if best.shrinkage_minute_scale is not None else "-"
            )
            best_suffix = (
                f"(alpha={best.ridge_alpha:.2f},mode={best.shrinkage_mode},"
                f"strength={best.shrinkage_strength:.2f},"
                f"minute_scale={minute_scale})"
            )
        lines.append(f"best_by_spearman={best.model}" + best_suffix)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.rawr_ridge_values = parse_float_grid(args.rawr_ridge_grid)
    args.shrinkage_strength_values = parse_float_grid(args.shrinkage_strength_grid)
    args.shrinkage_minute_scale_values = parse_float_grid(args.shrinkage_minute_scale_grid)
    if args.top_n <= 0:
        raise ValueError("top_n must be positive")
    if args.holdout_season in set(args.train_season):
        raise ValueError("holdout season must not be included in training seasons")

    print_status_box(
        "RAWR Tuning",
        [
            (
                f"Training on {', '.join(args.train_season)} and evaluating against "
                f"holdout {args.holdout_season}."
            ),
            (
                "The progress bar below tracks holdout preparation, baseline WOWY, "
                "and each RAWR configuration in the requested sweep."
            ),
        ],
    )
    results = evaluate_configs(args)
    print(build_summary(args, results))
    print(format_results_table(results))
    return 0
