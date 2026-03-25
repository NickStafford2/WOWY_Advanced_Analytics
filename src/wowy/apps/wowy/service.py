from __future__ import annotations

from wowy.apps.wowy.analysis import ProgressFn, compute_wowy, filter_results
from wowy.apps.wowy.minutes import (
    attach_minute_stats,
    filter_results_by_minutes,
    load_player_minute_stats,
)
from wowy.apps.wowy.formatting import format_results_table
from wowy.apps.wowy.models import WowyGameRecord
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.prepare import load_wowy_game_records
from wowy.progress import TerminalProgressBar, print_status_box
from wowy.shared.filters import validate_top_n_and_minutes
from wowy.shared.scope import format_scope

__all__ = [
    "build_wowy_report",
    "prepare_and_run_wowy",
    "run_wowy_records",
    "validate_filters",
]


def validate_filters(
    min_games_with: int,
    min_games_without: int,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> None:
    if min_games_with < 0 or min_games_without < 0:
        raise ValueError("Minimum game filters must be non-negative")
    validate_top_n_and_minutes(
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def build_wowy_report(
    games: list[WowyGameRecord],
    min_games_with: int,
    min_games_without: int,
    player_names: dict[int, str] | None = None,
    top_n: int | None = None,
    player_minute_stats: dict[int, tuple[float, float]] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    show_progress: bool = False,
) -> str:
    """Score WOWY from derived game rows, then apply output filters and formatting."""
    progress_bar = None
    progress: ProgressFn | None = None
    if show_progress:
        all_players = {player_id for game in games for player_id in game.players}
        progress_bar = TerminalProgressBar("WOWY", total=len(all_players))

        def _report_progress(current: int, _total: int, detail: str | None) -> None:
            progress_bar.update(current, detail)
        progress = _report_progress
    results = compute_wowy(games, progress=progress)
    if progress_bar is not None:
        progress_bar.finish("done")
    filtered_results = filter_results(
        results,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
    )
    filtered_results = filter_results_by_minutes(
        filtered_results,
        player_minute_stats=player_minute_stats,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    filtered_results = attach_minute_stats(filtered_results, player_minute_stats)
    return format_results_table(
        filtered_results,
        player_names=player_names,
        top_n=top_n,
    )


def run_wowy_records(
    games: list[WowyGameRecord],
    min_games_with: int,
    min_games_without: int,
    player_names: dict[int, str] | None = None,
    top_n: int | None = None,
    player_minute_stats: dict[int, tuple[float, float]] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    show_progress: bool = False,
) -> str:
    """Run WOWY from preloaded derived game records."""
    validate_filters(
        min_games_with,
        min_games_without,
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return build_wowy_report(
        games,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        player_names=player_names,
        top_n=top_n,
        player_minute_stats=player_minute_stats,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        show_progress=show_progress,
    )

def prepare_and_run_wowy(
    args,
) -> str:
    """CLI entrypoint for WOWY using the cache-managed pipeline."""
    validate_filters(
        args.min_games_with,
        args.min_games_without,
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    print_status_box(
        "WOWY CLI",
        [
            f"Scope: {format_scope(args.team, args.season)}",
            "Preparing cached game rows, rebuilding minute summaries, and then"
            " computing with/without player impact across the requested sample.",
            "The progress bar below tracks the player-by-player WOWY pass.",
        ],
    )
    print(f"[1/3] preparing WOWY inputs for {format_scope(args.team, args.season)}")
    games, player_names = load_wowy_game_records(
        teams=args.team,
        seasons=args.season,
        season_type=args.season_type,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
    )
    player_minute_stats = load_player_minute_stats(
        teams=args.team,
        seasons=args.season,
        season_type=args.season_type,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
    )
    print(f"[2/3] loaded {len(games)} WOWY game rows from cache")
    print("[3/3] computing WOWY results")
    return run_wowy_records(
        games,
        min_games_with=args.min_games_with,
        min_games_without=args.min_games_without,
        player_names=player_names,
        top_n=args.top_n,
        player_minute_stats=player_minute_stats,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
        show_progress=True,
    )
