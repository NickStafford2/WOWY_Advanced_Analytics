from __future__ import annotations

from wowy.apps.rawr.analysis import ProgressFn, fit_player_rawr, tune_ridge_alpha
from wowy.apps.rawr.data import (
    attach_minute_stats_to_result,
    build_player_season_minute_stats,
    build_rawr_observations,
    count_player_games,
    filter_rawr_estimates_by_minutes,
    filter_rawr_scope,
    select_complete_rawr_scope_seasons,
)
from wowy.apps.rawr.formatting import format_rawr_results
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.prepare import load_normalized_scope_records
from wowy.progress import TerminalProgressBar, print_status_box
from wowy.shared.filters import validate_top_n_and_minutes
from wowy.shared.scope import format_scope


def validate_filters(
    min_games: int,
    ridge_alpha: float,
    shrinkage_mode: str = "uniform",
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> None:
    if min_games < 0:
        raise ValueError("Minimum games filter must be non-negative")
    if ridge_alpha < 0:
        raise ValueError("Ridge alpha must be non-negative")
    if shrinkage_mode not in {"uniform", "game-count", "minutes"}:
        raise ValueError("Shrinkage mode must be 'uniform', 'game-count', or 'minutes'")
    if shrinkage_strength < 0:
        raise ValueError("Shrinkage strength must be non-negative")
    if shrinkage_minute_scale <= 0:
        raise ValueError("Shrinkage minute scale must be positive")
    validate_top_n_and_minutes(
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def run_rawr_records(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    min_games: int,
    ridge_alpha: float = 1.0,
    shrinkage_mode: str = "uniform",
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    top_n: int | None = None,
    teams: list[str] | None = None,
    seasons: list[str] | None = None,
    player_minute_stats: dict[tuple[str, int], tuple[float, float]] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    show_progress: bool = False,
) -> str:
    """Fit the game-level RAWR model from preloaded normalized records."""
    validate_filters(
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    games, game_players = filter_rawr_scope(
        games,
        game_players,
        teams=teams,
        seasons=seasons,
    )
    if player_minute_stats is None:
        player_minute_stats = build_player_season_minute_stats(games, game_players)
    observations, player_names = build_rawr_observations(games, game_players)
    progress_bar = None
    progress: ProgressFn | None = None
    if show_progress:
        player_count = sum(
            1
            for games_played in count_player_games(observations).values()
            if games_played >= min_games
        )
        team_seasons = {
            f"{_require_team_id(game.team_id, game.game_id, 'team_id')}:{game.season}"
            for game in games
        } | {
            f"{_require_team_id(game.opponent_team_id, game.game_id, 'opponent_team_id')}"
            f":{game.season}"
            for game in games
        }
        feature_count = 2 + player_count + (2 * len(team_seasons))
        total_steps = (
            (len(observations) * 2) + max(feature_count - 2, 0) + feature_count
        )
        progress_bar = TerminalProgressBar("RAWR", total=total_steps)
        def _report_progress(current: int, _total: int, detail: str | None) -> None:
            progress_bar.update(current, detail)
        progress = _report_progress
    result = fit_player_rawr(
        observations,
        player_names=player_names,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
        progress=progress,
    )
    if progress_bar is not None:
        progress_bar.finish("done")
    result = attach_minute_stats_to_result(result, player_minute_stats)
    result = filter_rawr_estimates_by_minutes(
        result,
        player_minute_stats=player_minute_stats,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return format_rawr_results(result, top_n=top_n)


def parse_ridge_grid(raw_value: str) -> list[float]:
    values = [part.strip() for part in raw_value.split(",")]
    if not values or any(not value for value in values):
        raise ValueError("Ridge grid must contain one or more comma-separated numbers")

    alphas = [float(value) for value in values]
    if any(alpha < 0.0 for alpha in alphas):
        raise ValueError("Ridge grid values must be non-negative")
    return alphas


def build_tuning_report(best_alpha: float, results) -> str:
    lines = [
        "Ridge tuning results",
        "-" * 34,
        f"{'alpha':>10} {'validation_mse':>16}",
        "-" * 34,
    ]
    for result in sorted(results, key=lambda item: item.alpha):
        marker = " *" if result.alpha == best_alpha else ""
        lines.append(f"{result.alpha:>10.4f} {result.validation_mse:>16.4f}{marker}")
    return "\n".join(lines)


def _require_team_id(team_id: int | None, game_id: str, field_name: str) -> int:
    if team_id is None or team_id <= 0:
        raise ValueError(
            f"Normalized cache-backed game {game_id!r} is missing {field_name}"
        )
    return team_id


def prepare_and_run_rawr(args) -> str:
    """CLI entrypoint for RAWR using the cache-managed pipeline."""
    validate_filters(
        min_games=args.min_games,
        ridge_alpha=args.ridge_alpha,
        shrinkage_mode=args.shrinkage_mode,
        shrinkage_strength=args.shrinkage_strength,
        shrinkage_minute_scale=args.shrinkage_minute_scale,
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    ridge_alpha = args.ridge_alpha
    print_status_box(
        "RAWR CLI",
        [
            f"Scope: {format_scope(args.team, args.season)}",
            "Preparing normalized game inputs, then fitting the ridge"
            " regression with team-season controls and minute-weighted player"
            " features.",
            "The progress bar below tracks matrix construction and linear-system"
            " solving for the requested sample.",
        ],
    )
    complete_seasons = select_complete_rawr_scope_seasons(
        teams=args.team,
        seasons=args.season,
        team_ids=None,
        season_type=args.season_type,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
    )
    if not complete_seasons:
        raise ValueError("No complete cached seasons matched the requested RAWR scope")
    print(f"[1/3] preparing RAWR inputs for {format_scope(args.team, args.season)}")
    games, game_players = load_normalized_scope_records(
        teams=args.team,
        seasons=complete_seasons,
        season_type=args.season_type,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
        include_opponents_for_team_scope=True,
    )
    print(
        "[2/3] loaded "
        f"{len(games)} normalized game rows and {len(game_players)} player rows from cache"
    )
    if args.tune_ridge:
        print("[3/4] tuning ridge alpha on a validation split")
        games, game_players = filter_rawr_scope(
            games,
            game_players,
            teams=args.team,
            seasons=args.season,
        )
        observations, player_names = build_rawr_observations(games, game_players)
        tuning_summary = tune_ridge_alpha(
            observations,
            player_names=player_names,
            alphas=parse_ridge_grid(args.ridge_grid),
            min_games=args.min_games,
            validation_fraction=args.validation_fraction,
            shrinkage_mode=args.shrinkage_mode,
            shrinkage_strength=args.shrinkage_strength,
            shrinkage_minute_scale=args.shrinkage_minute_scale,
        )
        ridge_alpha = tuning_summary.best_alpha
        print(build_tuning_report(tuning_summary.best_alpha, tuning_summary.results))
        print(f"selected ridge alpha: {ridge_alpha:.4f}")
        print("[4/4] fitting RAWR model")
    else:
        print("[3/3] fitting RAWR model")
    return run_rawr_records(
        games,
        game_players,
        min_games=args.min_games,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=args.shrinkage_mode,
        shrinkage_strength=args.shrinkage_strength,
        shrinkage_minute_scale=args.shrinkage_minute_scale,
        top_n=args.top_n,
        teams=args.team,
        seasons=args.season,
        player_minute_stats=None,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
        show_progress=True,
    )
