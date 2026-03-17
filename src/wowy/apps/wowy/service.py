from __future__ import annotations

from pathlib import Path
from typing import Callable

from wowy.apps.wowy.analysis import compute_wowy, filter_results
from wowy.apps.wowy.formatting import format_results_table
from wowy.apps.wowy.models import (
    WowyGameRecord,
    WowyPlayerSeasonRecord,
    WowyPlayerStats,
)
from wowy.data.wowy_io import load_games_from_csv, write_player_season_records_csv
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest import load_player_names_from_cache
from wowy.nba.prepare import (
    prepare_normalized_scope_records,
    prepare_wowy_game_records,
    prepare_wowy_inputs,
)
from wowy.progress import TerminalProgressBar, print_status_box
from wowy.shared.filters import validate_top_n_and_minutes
from wowy.shared.minutes import build_player_minute_stats, passes_minute_filters
from wowy.shared.scope import format_scope


type LoadPlayerNamesFn = Callable[[Path], dict[int, str]]
type WowyPlayerSeasonRow = dict[str, str | int | float | None]


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
    season_type: str = "Regular Season",
    source_data_dir: Path | None = None,
    wowy_output_dir: Path | None = None,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> dict[int, tuple[float, float]]:
    """Build minute summaries from the DB-backed normalized cache with CSV fallback."""
    _games, game_players = prepare_normalized_scope_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        source_data_dir=source_data_dir
        or Path("data/source/nba"),
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir or Path("data/raw/nba/team_games"),
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
        log=lambda *_args, **_kwargs: None,
    )
    return build_player_minute_stats(game_players)


def load_player_season_minute_stats(
    teams: list[str] | None,
    seasons: list[str] | None,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    season_type: str = "Regular Season",
    source_data_dir: Path | None = None,
    wowy_output_dir: Path | None = None,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> dict[tuple[str, int], tuple[float, float]]:
    totals: dict[tuple[str, int], float] = {}
    counts: dict[tuple[str, int], int] = {}

    games, game_players = prepare_normalized_scope_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        source_data_dir=source_data_dir or Path("data/source/nba"),
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir or Path("data/raw/nba/team_games"),
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
        log=lambda *_args, **_kwargs: None,
    )
    seasons_by_game_id = {game.game_id: game.season for game in games}
    for player in game_players:
        season = seasons_by_game_id.get(player.game_id)
        if season is None or not player.appeared or player.minutes is None or player.minutes <= 0.0:
            continue
        key = (season, player.player_id)
        totals[key] = totals.get(key, 0.0) + player.minutes
        counts[key] = counts.get(key, 0) + 1

    return {
        key: (totals[key] / counts[key], totals[key])
        for key in totals
    }


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


def build_wowy_player_season_records(
    games: list[WowyGameRecord],
    min_games_with: int,
    min_games_without: int,
    player_names: dict[int, str] | None = None,
    player_season_minute_stats: dict[tuple[str, int], tuple[float, float]] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[WowyPlayerSeasonRecord]:
    validate_filters(
        min_games_with,
        min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    player_names = player_names or {}
    games_by_season: dict[str, list[WowyGameRecord]] = {}
    for game in games:
        games_by_season.setdefault(game.season, []).append(game)

    records: list[WowyPlayerSeasonRecord] = []
    for season in sorted(games_by_season):
        results = compute_wowy(games_by_season[season])
        results = filter_results(
            results,
            min_games_with=min_games_with,
            min_games_without=min_games_without,
        )

        season_minute_stats = None
        if player_season_minute_stats is not None:
            season_minute_stats = {
                player_id: stats
                for (row_season, player_id), stats in player_season_minute_stats.items()
                if row_season == season
            }

        results = filter_results_by_minutes(
            results,
            player_minute_stats=season_minute_stats,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        results = attach_minute_stats(results, season_minute_stats)

        ranked = sorted(
            results.items(),
            key=lambda item: item[1].wowy_score if item[1].wowy_score is not None else float("-inf"),
            reverse=True,
        )
        for player_id, stats in ranked:
            if (
                stats.avg_margin_with is None
                or stats.avg_margin_without is None
                or stats.wowy_score is None
            ):
                continue
            records.append(
                WowyPlayerSeasonRecord(
                    season=season,
                    player_id=player_id,
                    player_name=player_names.get(player_id, str(player_id)),
                    games_with=stats.games_with,
                    games_without=stats.games_without,
                    avg_margin_with=stats.avg_margin_with,
                    avg_margin_without=stats.avg_margin_without,
                    wowy_score=stats.wowy_score,
                    average_minutes=stats.average_minutes,
                    total_minutes=stats.total_minutes,
                )
            )

    return records


def serialize_wowy_player_season_records(
    records: list[WowyPlayerSeasonRecord],
) -> list[WowyPlayerSeasonRow]:
    return [
        {
            "season": record.season,
            "player_id": record.player_id,
            "player_name": record.player_name,
            "games_with": record.games_with,
            "games_without": record.games_without,
            "avg_margin_with": record.avg_margin_with,
            "avg_margin_without": record.avg_margin_without,
            "wowy_score": record.wowy_score,
            "average_minutes": record.average_minutes,
            "total_minutes": record.total_minutes,
        }
        for record in records
    ]


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
        record
        for record in records
        if start_season <= record.season <= end_season
    ]
    if not span_records:
        return []

    score_totals: dict[int, float] = {}
    season_counts: dict[int, int] = {}
    player_names: dict[int, str] = {}
    season_scores: dict[int, dict[str, float]] = {}

    for record in span_records:
        score_totals[record.player_id] = score_totals.get(record.player_id, 0.0) + record.wowy_score
        season_counts[record.player_id] = season_counts.get(record.player_id, 0) + 1
        player_names[record.player_id] = record.player_name
        season_scores.setdefault(record.player_id, {})[record.season] = record.wowy_score

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


def available_wowy_seasons(
    records: list[WowyPlayerSeasonRecord],
) -> list[str]:
    return sorted({record.season for record in records})


def prepare_wowy_player_season_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
    min_games_with: int,
    min_games_without: int,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    load_player_names_fn: LoadPlayerNamesFn = load_player_names_from_cache,
) -> list[WowyPlayerSeasonRecord]:
    validate_filters(
        min_games_with,
        min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    games, player_names = prepare_wowy_game_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        player_metrics_db_path=player_metrics_db_path,
        log=lambda *_args, **_kwargs: None,
    )
    player_season_minute_stats = load_player_season_minute_stats(
        teams=teams,
        seasons=seasons,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        season_type=season_type,
        source_data_dir=source_data_dir,
        wowy_output_dir=wowy_output_dir,
        player_metrics_db_path=player_metrics_db_path,
    )
    return build_wowy_player_season_records(
        games,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        player_names=player_names,
        player_season_minute_stats=player_season_minute_stats,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def prepare_and_run_wowy(
    args,
    load_player_names_fn: LoadPlayerNamesFn = load_player_names_from_cache,
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
    csv_path, player_names = prepare_wowy_inputs(
        teams=args.team,
        seasons=args.season,
        combined_wowy_csv=args.combined_wowy_csv,
        season_type=args.season_type,
        source_data_dir=args.source_data_dir,
        normalized_games_input_dir=args.normalized_games_input_dir,
        normalized_game_players_input_dir=args.normalized_game_players_input_dir,
        wowy_output_dir=args.wowy_output_dir,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
    )
    player_minute_stats = load_player_minute_stats(
        teams=args.team,
        seasons=args.season,
        normalized_games_input_dir=args.normalized_games_input_dir,
        normalized_game_players_input_dir=args.normalized_game_players_input_dir,
        season_type=args.season_type,
        source_data_dir=args.source_data_dir,
        wowy_output_dir=args.wowy_output_dir,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
    )
    print(f"[2/3] running WOWY from {csv_path}")
    print("[3/3] computing WOWY results")
    if args.export_player_seasons is not None:
        records = prepare_wowy_player_season_records(
            teams=args.team,
            seasons=args.season,
            season_type=args.season_type,
            source_data_dir=args.source_data_dir,
            normalized_games_input_dir=args.normalized_games_input_dir,
            normalized_game_players_input_dir=args.normalized_game_players_input_dir,
            wowy_output_dir=args.wowy_output_dir,
            combined_wowy_csv=args.combined_wowy_csv,
            player_metrics_db_path=getattr(
                args,
                "player_metrics_db_path",
                DEFAULT_PLAYER_METRICS_DB_PATH,
            ),
            min_games_with=args.min_games_with,
            min_games_without=args.min_games_without,
            min_average_minutes=args.min_average_minutes,
            min_total_minutes=args.min_total_minutes,
            load_player_names_fn=load_player_names_fn,
        )
        write_player_season_records_csv(args.export_player_seasons, records)
        print(
            f"exported {len(records)} player-season WOWY rows to "
            f"{args.export_player_seasons}"
        )
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
