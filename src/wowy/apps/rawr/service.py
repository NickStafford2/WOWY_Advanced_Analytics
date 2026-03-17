from __future__ import annotations

from pathlib import Path

from wowy.apps.rawr.analysis import fit_player_rawr, tune_ridge_alpha
from wowy.apps.rawr.data import build_rawr_observations, count_player_games
from wowy.apps.rawr.formatting import format_rawr_results
from wowy.apps.rawr.models import (
    RawrPlayerEstimate,
    RawrPlayerSeasonRecord,
    RawrResult,
)
from wowy.nba.prepare import prepare_rawr_inputs
from wowy.nba.team_seasons import resolve_team_seasons
from wowy.data.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)
from wowy.progress import TerminalProgressBar, print_status_box
from wowy.shared.filters import validate_top_n_and_minutes
from wowy.shared.minutes import passes_minute_filters
from wowy.shared.scope import format_scope


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
    validate_top_n_and_minutes(
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def attach_minute_stats_to_result(
    result: RawrResult,
    player_minute_stats: dict[tuple[str, int], tuple[float, float]] | None,
) -> RawrResult:
    if player_minute_stats is None:
        return result

    estimates = [
        RawrPlayerEstimate(
            season=estimate.season,
            player_id=estimate.player_id,
            player_name=estimate.player_name,
            games=estimate.games,
            average_minutes=player_minute_stats.get(
                (estimate.season, estimate.player_id),
                (None, None),
            )[0],
            total_minutes=player_minute_stats.get(
                (estimate.season, estimate.player_id),
                (None, None),
            )[1],
            coefficient=estimate.coefficient,
        )
        for estimate in result.estimates
    ]
    return RawrResult(
        observations=result.observations,
        players=result.players,
        intercept=result.intercept,
        home_court_advantage=result.home_court_advantage,
        estimates=estimates,
    )


def filter_rawr_estimates_by_minutes(
    result: RawrResult,
    player_minute_stats: dict[tuple[str, int], tuple[float, float]] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> RawrResult:
    if player_minute_stats is None:
        return result
    if min_average_minutes is None and min_total_minutes is None:
        return result

    filtered_estimates = [
        estimate
        for estimate in result.estimates
        if passes_minute_filters(
            player_minute_stats.get((estimate.season, estimate.player_id)),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    ]

    return RawrResult(
        observations=result.observations,
        players=result.players,
        intercept=result.intercept,
        home_court_advantage=result.home_court_advantage,
        estimates=filtered_estimates,
    )


def filter_rawr_scope(
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
        raise ValueError("No games matched the requested RAWR scope")

    filtered_games = [game for game in games if game.game_id in selected_game_ids]
    filtered_game_players = [
        player for player in game_players if player.game_id in selected_game_ids
    ]
    return filtered_games, filtered_game_players


def run_rawr(
    games_csv_path: Path | str,
    game_players_csv_path: Path | str,
    min_games: int,
    ridge_alpha: float = 1.0,
    top_n: int | None = None,
    teams: list[str] | None = None,
    seasons: list[str] | None = None,
    player_minute_stats: dict[tuple[str, int], tuple[float, float]] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    show_progress: bool = False,
) -> str:
    """Fit the game-level RAWR model from normalized CSV inputs."""
    validate_filters(
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )

    games = load_normalized_games_from_csv(games_csv_path)
    game_players = load_normalized_game_players_from_csv(game_players_csv_path)
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
    progress = None
    if show_progress:
        player_count = sum(
            1
            for games_played in count_player_games(observations).values()
            if games_played >= min_games
        )
        team_seasons = {f"{game.team}:{game.season}" for game in games} | {
            f"{game.opponent}:{game.season}" for game in games
        }
        feature_count = 2 + player_count + (2 * len(team_seasons))
        total_steps = (
            (len(observations) * 2) + max(feature_count - 2, 0) + feature_count
        )
        progress_bar = TerminalProgressBar("RAWR", total=total_steps)
        progress = lambda current, _total, detail: progress_bar.update(current, detail)
    result = fit_player_rawr(
        observations,
        player_names=player_names,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
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


def build_player_season_minute_stats(
    games,
    game_players,
) -> dict[tuple[str, int], tuple[float, float]]:
    season_by_game_id = {
        game.game_id: game.season
        for game in games
    }
    totals: dict[tuple[str, int], float] = {}
    counts: dict[tuple[str, int], int] = {}

    for player in game_players:
        season = season_by_game_id.get(player.game_id)
        if (
            season is None
            or not player.appeared
            or player.minutes is None
            or player.minutes <= 0.0
        ):
            continue
        key = (season, player.player_id)
        totals[key] = totals.get(key, 0.0) + player.minutes
        counts[key] = counts.get(key, 0) + 1

    return {
        key: (totals[key] / counts[key], totals[key])
        for key in totals
    }


def prepare_rawr_player_season_records(
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    combined_games_csv: Path,
    combined_game_players_csv: Path,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    min_games: int,
    ridge_alpha: float,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> list[RawrPlayerSeasonRecord]:
    validate_filters(
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    team_seasons = resolve_team_seasons(teams, seasons, normalized_games_input_dir)
    teams_by_season: dict[str, list[str]] = {}
    for team_season in team_seasons:
        teams_by_season.setdefault(team_season.season, []).append(team_season.team)
    records: list[RawrPlayerSeasonRecord] = []

    for season in sorted(teams_by_season):
        games_csv, game_players_csv = prepare_rawr_inputs(
            teams=sorted(set(teams_by_season[season])),
            seasons=[season],
            combined_games_csv=combined_games_csv,
            combined_game_players_csv=combined_game_players_csv,
            season_type=season_type,
            source_data_dir=source_data_dir,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            log=lambda *_args, **_kwargs: None,
        )
        games = load_normalized_games_from_csv(games_csv)
        game_players = load_normalized_game_players_from_csv(game_players_csv)
        try:
            games, game_players = filter_rawr_scope(
                games,
                game_players,
                teams=sorted(set(teams_by_season[season])),
                seasons=[season],
            )
        except ValueError as exc:
            if str(exc) == "No games matched the requested RAWR scope":
                continue
            raise
        player_minute_stats = build_player_season_minute_stats(games, game_players)
        observations, player_names = build_rawr_observations(games, game_players)
        try:
            result = fit_player_rawr(
                observations,
                player_names=player_names,
                min_games=min_games,
                ridge_alpha=ridge_alpha,
            )
        except ValueError as exc:
            if str(exc) == "No players met the minimum games requirement":
                continue
            raise
        result = attach_minute_stats_to_result(result, player_minute_stats)
        result = filter_rawr_estimates_by_minutes(
            result,
            player_minute_stats=player_minute_stats,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        for estimate in result.estimates:
            records.append(
                RawrPlayerSeasonRecord(
                    season=season,
                    player_id=estimate.player_id,
                    player_name=estimate.player_name,
                    games=estimate.games,
                    average_minutes=estimate.average_minutes,
                    total_minutes=estimate.total_minutes,
                    coefficient=estimate.coefficient,
                )
            )

    records.sort(
        key=lambda record: (record.season, record.coefficient, record.player_name),
        reverse=True,
    )
    return records


def prepare_and_run_rawr(args) -> str:
    """CLI entrypoint for RAWR using the cache-managed pipeline."""
    validate_filters(
        min_games=args.min_games,
        ridge_alpha=args.ridge_alpha,
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
    print(
        f"[1/3] preparing RAWR inputs for {format_scope(args.team, args.season)}"
    )
    games_csv, game_players_csv = prepare_rawr_inputs(
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
    print(f"[2/3] loading RAWR data from {games_csv} and {game_players_csv}")
    if args.tune_ridge:
        print("[3/4] tuning ridge alpha on a validation split")
        games = load_normalized_games_from_csv(games_csv)
        game_players = load_normalized_game_players_from_csv(game_players_csv)
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
        )
        ridge_alpha = tuning_summary.best_alpha
        print(build_tuning_report(tuning_summary.best_alpha, tuning_summary.results))
        print(f"selected ridge alpha: {ridge_alpha:.4f}")
        print("[4/4] fitting RAWR model")
    else:
        print("[3/3] fitting RAWR model")
    return run_rawr(
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
