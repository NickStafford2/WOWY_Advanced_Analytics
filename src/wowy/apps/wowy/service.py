from __future__ import annotations

from wowy.apps.wowy.analysis import compute_wowy, filter_results
from wowy.apps.wowy.data import (
    attach_minute_stats,
    available_wowy_seasons,
    build_wowy_player_season_records,
    filter_results_by_minutes,
    load_player_minute_stats,
    load_player_season_minute_stats,
    prepare_wowy_player_season_records,
    serialize_wowy_player_season_records,
)
from wowy.apps.wowy.formatting import format_results_table
from wowy.apps.wowy.models import (
    WowyGameRecord,
    WowyPlayerSeasonRecord,
    WowyPlayerStats,
)
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.prepare import load_wowy_game_records
from wowy.progress import TerminalProgressBar, print_status_box
from wowy.shared.filters import validate_top_n_and_minutes
from wowy.shared.scope import format_scope


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


def build_wowy_span_chart_rows(
    records: list[WowyPlayerSeasonRecord],
    *,
    start_season: str,
    end_season: str,
    top_n: int = 30,
) -> list[dict[str, str | int | float | list[dict[str, str | float | None]]]]:
    validate_top_n_and_minutes(top_n=top_n)
    if start_season > end_season:
        raise ValueError("start_season must be less than or equal to end_season")

    span_records = [
        record for record in records if start_season <= record.season <= end_season
    ]
    if not span_records:
        return []

    score_totals: dict[int, float] = {}
    season_counts: dict[int, int] = {}
    player_names: dict[int, str] = {}
    season_scores: dict[int, dict[str, float]] = {}

    for record in span_records:
        score_totals[record.player_id] = (
            score_totals.get(record.player_id, 0.0) + record.wowy_score
        )
        season_counts[record.player_id] = season_counts.get(record.player_id, 0) + 1
        player_names[record.player_id] = record.player_name
        season_scores.setdefault(record.player_id, {})[record.season] = (
            record.wowy_score
        )

    ranked_player_ids = sorted(
        score_totals,
        key=lambda player_id: (
            score_totals[player_id],
            player_names[player_id],
        ),
        reverse=True,
    )[:top_n]
    seasons = sorted({record.season for record in span_records})
    span_length = len(seasons)

    return [
        {
            "player_id": player_id,
            "player_name": player_names[player_id],
            "span_average_value": score_totals[player_id] / span_length,
            "season_count": season_counts[player_id],
            "points": [
                {
                    "season": season,
                    "value": season_scores[player_id].get(season),
                }
                for season in seasons
            ],
        }
        for player_id in ranked_player_ids
    ]


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
