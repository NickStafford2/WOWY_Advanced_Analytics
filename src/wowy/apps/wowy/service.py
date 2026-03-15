from __future__ import annotations

from pathlib import Path

from wowy.apps.wowy.analysis import compute_wowy, filter_results
from wowy.apps.wowy.formatting import format_results_table
from wowy.data.wowy_io import load_games_from_csv
from wowy.nba.ingest import load_player_names_from_cache
from wowy.nba.paths import normalized_game_players_path
from wowy.nba.prepare import prepare_wowy_inputs
from wowy.nba.team_seasons import resolve_team_seasons
from wowy.data.normalized_io import load_normalized_game_players_from_csv
from wowy.progress import TerminalProgressBar
from wowy.shared.filters import validate_top_n_and_minutes
from wowy.shared.minutes import build_player_minute_stats, passes_minute_filters
from wowy.shared.scope import format_scope
from wowy.apps.wowy.models import WowyGameRecord, WowyPlayerStats


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
    progress = None
    if show_progress:
        all_players = {player_id for game in games for player_id in game.players}
        progress_bar = TerminalProgressBar("WOWY", total=len(all_players))
        progress = lambda current, _total, detail: progress_bar.update(current, detail)
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


def run_wowy(
    csv_path: Path | str,
    min_games_with: int,
    min_games_without: int,
    player_names: dict[int, str] | None = None,
    top_n: int | None = None,
    player_minute_stats: dict[int, tuple[float, float]] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    show_progress: bool = False,
) -> str:
    """Run WOWY from a derived `games.csv` input."""
    validate_filters(
        min_games_with,
        min_games_without,
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    games = load_games_from_csv(csv_path)
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


def load_player_minute_stats(
    teams: list[str] | None,
    seasons: list[str] | None,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
) -> dict[int, tuple[float, float]]:
    """Build minute summaries from normalized cache files for WOWY output filtering."""
    game_players = []
    for team_season in resolve_team_seasons(teams, seasons, normalized_games_input_dir):
        game_players.extend(
            load_normalized_game_players_from_csv(
                normalized_game_players_path(
                    team_season,
                    normalized_game_players_input_dir,
                )
            )
        )
    return build_player_minute_stats(game_players)


def filter_results_by_minutes(
    results: dict[int, WowyPlayerStats],
    player_minute_stats: dict[int, tuple[float, float]] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[int, WowyPlayerStats]:
    if player_minute_stats is None:
        return results
    if min_average_minutes is None and min_total_minutes is None:
        return results

    return {
        player_id: stats
        for player_id, stats in results.items()
        if passes_minute_filters(
            player_minute_stats.get(player_id),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    }


def attach_minute_stats(
    results: dict[int, WowyPlayerStats],
    player_minute_stats: dict[int, tuple[float, float]] | None,
) -> dict[int, WowyPlayerStats]:
    if player_minute_stats is None:
        return results

    updated = {}
    for player_id, stats in results.items():
        average_minutes, total_minutes = player_minute_stats.get(
            player_id,
            (None, None),
        )
        updated[player_id] = WowyPlayerStats(
            games_with=stats.games_with,
            games_without=stats.games_without,
            avg_margin_with=stats.avg_margin_with,
            avg_margin_without=stats.avg_margin_without,
            wowy_score=stats.wowy_score,
            average_minutes=average_minutes,
            total_minutes=total_minutes,
        )
    return updated


def prepare_and_run_wowy(
    args,
    load_player_names_fn=load_player_names_from_cache,
) -> str:
    """CLI entrypoint for WOWY.

    Explicit `--csv` runs only the derived WOWY scorer.
    Cache-managed runs can also apply minute-based output filters using normalized data.
    """
    validate_filters(
        args.min_games_with,
        args.min_games_without,
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    print(f"[1/3] preparing WOWY inputs for {format_scope(args.team, args.season)}")
    player_minute_stats = None
    if args.csv is not None:
        csv_path = args.csv
        print(f"[2/3] loading WOWY games from {csv_path}")
        player_names = load_player_names_fn(args.source_data_dir)
        if args.min_average_minutes is not None or args.min_total_minutes is not None:
            raise ValueError(
                "Minutes-based WOWY filters require cache-managed inputs, not --csv"
            )
    else:
        csv_path, player_names = prepare_wowy_inputs(
            teams=args.team,
            seasons=args.season,
            combined_wowy_csv=args.combined_wowy_csv,
            season_type=args.season_type,
            source_data_dir=args.source_data_dir,
            normalized_games_input_dir=args.normalized_games_input_dir,
            normalized_game_players_input_dir=args.normalized_game_players_input_dir,
            wowy_output_dir=args.wowy_output_dir,
        )
        player_minute_stats = load_player_minute_stats(
            teams=args.team,
            seasons=args.season,
            normalized_games_input_dir=args.normalized_games_input_dir,
            normalized_game_players_input_dir=args.normalized_game_players_input_dir,
        )
        print(f"[2/3] running WOWY from {csv_path}")
    print("[3/3] computing WOWY results")
    return run_wowy(
        csv_path,
        min_games_with=args.min_games_with,
        min_games_without=args.min_games_without,
        player_names=player_names,
        top_n=args.top_n,
        player_minute_stats=player_minute_stats,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
        show_progress=True,
    )
