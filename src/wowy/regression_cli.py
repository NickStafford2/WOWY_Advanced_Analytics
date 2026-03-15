from __future__ import annotations

import argparse
from pathlib import Path

from wowy.cache_pipeline import prepare_regression_inputs
from wowy.ingest_nba import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)
from wowy.progress import TerminalProgressBar
from wowy.regression_analysis import fit_player_regression, tune_ridge_alpha
from wowy.regression_data import build_regression_observations, count_player_games
from wowy.regression_formatting import format_regression_results
from wowy.regression_types import RegressionPlayerEstimate, RegressionResult


def format_scope(teams: list[str] | None, seasons: list[str] | None) -> str:
    team_label = (
        ",".join(team.upper() for team in teams) if teams else "all cached teams"
    )
    season_label = ",".join(seasons) if seasons else "all cached seasons"
    return f"teams={team_label} seasons={season_label}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run regression on cached data, fetching missing requested scope when needed."
    )
    parser.add_argument(
        "--games-csv",
        type=Path,
        default=None,
        help="Optional explicit combined normalized games CSV path",
    )
    parser.add_argument(
        "--game-players-csv",
        type=Path,
        default=None,
        help="Optional explicit combined normalized game-player CSV path",
    )
    parser.add_argument(
        "--team",
        action="append",
        default=None,
        help="Filter to a team abbreviation. Repeat to include multiple teams.",
    )
    parser.add_argument(
        "--season",
        action="append",
        default=None,
        help="Filter to a season string. Repeat to include multiple seasons.",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to fetch when requested scope is missing from cache",
    )
    parser.add_argument(
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--normalized-games-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAMES_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--normalized-game-players-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--wowy-output-dir",
        type=Path,
        default=DEFAULT_WOWY_GAMES_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--combined-games-csv",
        type=Path,
        default=Path("data/combined/regression/games.csv"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--combined-game-players-csv",
        type=Path,
        default=Path("data/combined/regression/game_players.csv"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=35,
        help="Minimum games required to include a player in the regression output (default: 35)",
    )
    parser.add_argument(
        "--ridge-alpha",
        type=float,
        default=10.0,
        help="Ridge regularization strength for player coefficients",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=40,
        help="Maximum number of players to include in output",
    )
    parser.add_argument(
        "--tune-ridge",
        action="store_true",
        help="Choose ridge alpha from a validation split before fitting the final model",
    )
    parser.add_argument(
        "--ridge-grid",
        default="0.3,1,3,10,30,100",
        help="Comma-separated ridge alphas to evaluate when --tune-ridge is used",
    )
    parser.add_argument(
        "--validation-fraction",
        type=float,
        default=0.2,
        help="Fraction of games held out for ridge tuning when --tune-ridge is used",
    )
    parser.add_argument(
        "--min-average-minutes",
        type=float,
        default=30,
        help="Minimum average minutes per appeared game required to include a player in output",
    )
    parser.add_argument(
        "--min-total-minutes",
        type=float,
        default=600,
        help="Minimum total minutes required to include a player in output",
    )
    return parser


def validate_filters(
    min_games: int,
    ridge_alpha: float,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> None:
    if min_games < 0:
        raise ValueError("Minimum games filter must be non-negative")
    if ridge_alpha < 0:
        raise ValueError("Ridge alpha must be non-negative")
    if top_n is not None and top_n < 0:
        raise ValueError("Top-n filter must be non-negative")
    if min_average_minutes is not None and min_average_minutes < 0:
        raise ValueError("Minimum average minutes filter must be non-negative")
    if min_total_minutes is not None and min_total_minutes < 0:
        raise ValueError("Minimum total minutes filter must be non-negative")


def build_player_minute_stats(
    game_players,
) -> dict[int, tuple[float, float]]:
    totals: dict[int, float] = {}
    counts: dict[int, int] = {}

    for player in game_players:
        if not player.appeared or player.minutes is None or player.minutes <= 0.0:
            continue
        totals[player.player_id] = totals.get(player.player_id, 0.0) + player.minutes
        counts[player.player_id] = counts.get(player.player_id, 0) + 1

    return {
        player_id: (totals[player_id] / counts[player_id], totals[player_id])
        for player_id in totals
    }


def attach_minute_stats_to_result(
    result: RegressionResult,
    player_minute_stats: dict[int, tuple[float, float]] | None,
) -> RegressionResult:
    if player_minute_stats is None:
        return result

    estimates = [
        RegressionPlayerEstimate(
            player_id=estimate.player_id,
            player_name=estimate.player_name,
            games=estimate.games,
            average_minutes=player_minute_stats.get(estimate.player_id, (None, None))[
                0
            ],
            total_minutes=player_minute_stats.get(estimate.player_id, (None, None))[1],
            coefficient=estimate.coefficient,
        )
        for estimate in result.estimates
    ]
    return RegressionResult(
        observations=result.observations,
        players=result.players,
        intercept=result.intercept,
        home_court_advantage=result.home_court_advantage,
        estimates=estimates,
    )


def filter_regression_estimates_by_minutes(
    result: RegressionResult,
    player_minute_stats: dict[int, tuple[float, float]] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> RegressionResult:
    if player_minute_stats is None:
        return result
    if min_average_minutes is None and min_total_minutes is None:
        return result

    filtered_estimates = []
    for estimate in result.estimates:
        minute_stats = player_minute_stats.get(estimate.player_id)
        if minute_stats is None:
            continue
        average_minutes, total_minutes = minute_stats
        if min_average_minutes is not None and average_minutes < min_average_minutes:
            continue
        if min_total_minutes is not None and total_minutes < min_total_minutes:
            continue
        filtered_estimates.append(estimate)

    return RegressionResult(
        observations=result.observations,
        players=result.players,
        intercept=result.intercept,
        home_court_advantage=result.home_court_advantage,
        estimates=filtered_estimates,
    )


def run_regression(
    games_csv_path: Path | str,
    game_players_csv_path: Path | str,
    min_games: int,
    ridge_alpha: float = 1.0,
    top_n: int | None = None,
    teams: list[str] | None = None,
    seasons: list[str] | None = None,
    player_minute_stats: dict[int, tuple[float, float]] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    show_progress: bool = False,
) -> str:
    validate_filters(
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )

    games = load_normalized_games_from_csv(games_csv_path)
    game_players = load_normalized_game_players_from_csv(game_players_csv_path)
    games, game_players = filter_regression_scope(
        games,
        game_players,
        teams=teams,
        seasons=seasons,
    )
    if player_minute_stats is None:
        player_minute_stats = build_player_minute_stats(game_players)
    observations, player_names = build_regression_observations(games, game_players)
    progress_bar = None
    progress = None
    if show_progress:
        player_count = sum(
            1
            for games_played in count_player_games(observations).values()
            if games_played >= min_games
        )
        team_seasons = {
            f"{game.team}:{game.season}" for game in games
        } | {
            f"{game.opponent}:{game.season}" for game in games
        }
        feature_count = 2 + player_count + (2 * len(team_seasons))
        total_steps = (len(observations) * 2) + max(feature_count - 2, 0) + feature_count
        progress_bar = TerminalProgressBar("Regression", total=total_steps)
        progress = lambda current, _total, detail: progress_bar.update(current, detail)
    result = fit_player_regression(
        observations,
        player_names=player_names,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        progress=progress,
    )
    if progress_bar is not None:
        progress_bar.finish("done")
    result = attach_minute_stats_to_result(result, player_minute_stats)
    result = filter_regression_estimates_by_minutes(
        result,
        player_minute_stats=player_minute_stats,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return format_regression_results(result, top_n=top_n)


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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_filters(
        min_games=args.min_games,
        ridge_alpha=args.ridge_alpha,
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    games_csv = args.games_csv
    game_players_csv = args.game_players_csv
    ridge_alpha = args.ridge_alpha
    print(
        f"[1/3] preparing regression inputs for {format_scope(args.team, args.season)}"
    )
    if games_csv is None or game_players_csv is None:
        games_csv, game_players_csv = prepare_regression_inputs(
            teams=args.team,
            seasons=args.season,
            combined_games_csv=args.combined_games_csv,
            combined_game_players_csv=args.combined_game_players_csv,
            season_type=args.season_type,
            source_data_dir=args.source_data_dir,
            normalized_games_input_dir=args.normalized_games_input_dir,
            normalized_game_players_input_dir=args.normalized_game_players_input_dir,
            wowy_output_dir=args.wowy_output_dir,
        )
    print(f"[2/3] loading regression data from {games_csv} and {game_players_csv}")
    if args.tune_ridge:
        print("[3/4] tuning ridge alpha on a validation split")
        games = load_normalized_games_from_csv(games_csv)
        game_players = load_normalized_game_players_from_csv(game_players_csv)
        games, game_players = filter_regression_scope(
            games,
            game_players,
            teams=args.team,
            seasons=args.season,
        )
        observations, player_names = build_regression_observations(games, game_players)
        tuning_summary = tune_ridge_alpha(
            observations,
            player_names=player_names,
            alphas=parse_ridge_grid(args.ridge_grid),
            min_games=args.min_games,
            validation_fraction=args.validation_fraction,
        )
        ridge_alpha = tuning_summary.best_alpha
        print(build_tuning_report(tuning_summary.best_alpha, tuning_summary.results))
        print(f"selected ridge alpha: {ridge_alpha:.4f}")
        print("[4/4] fitting regression model")
    else:
        print("[3/3] fitting regression model")
    print(
        run_regression(
            games_csv,
            game_players_csv,
            min_games=args.min_games,
            ridge_alpha=ridge_alpha,
            top_n=args.top_n,
            teams=args.team,
            seasons=args.season,
            player_minute_stats=None,
            min_average_minutes=args.min_average_minutes,
            min_total_minutes=args.min_total_minutes,
            show_progress=True,
        )
    )
    return 0


def filter_regression_scope(
    games,
    game_players,
    teams: list[str] | None,
    seasons: list[str] | None,
):
    if not teams and not seasons:
        return games, game_players

    normalized_teams = {team.upper() for team in teams or []}
    normalized_seasons = set(seasons or [])
    selected_game_ids = {
        game.game_id
        for game in games
        if (not normalized_seasons or game.season in normalized_seasons)
        and (not normalized_teams or game.team in normalized_teams)
    }
    if not selected_game_ids:
        raise ValueError("No games matched the requested regression scope")

    filtered_games = [game for game in games if game.game_id in selected_game_ids]
    filtered_game_players = [
        player for player in game_players if player.game_id in selected_game_ids
    ]
    return filtered_games, filtered_game_players


if __name__ == "__main__":
    raise SystemExit(main())
