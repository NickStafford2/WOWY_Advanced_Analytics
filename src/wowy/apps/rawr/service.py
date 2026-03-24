from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass

from wowy.apps.rawr.analysis import fit_player_rawr, tune_ridge_alpha
from wowy.apps.rawr.data import build_rawr_observations, count_player_games
from wowy.apps.rawr.formatting import format_rawr_results
from wowy.apps.rawr.models import (
    RawrPlayerEstimate,
    RawrPlayerSeasonRecord,
    RawrResult,
)
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.data.game_cache_db import list_cache_load_rows
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.prepare import prepare_canonical_scope_records
from wowy.nba.team_identity import list_expected_team_abbreviations_for_season, resolve_team_id
from wowy.nba.team_seasons import resolve_team_seasons
from wowy.progress import TerminalProgressBar, print_status_box
from wowy.shared.filters import validate_top_n_and_minutes
from wowy.shared.minutes import passes_minute_filters
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
    team_ids: list[int] | None = None,
):
    if not teams and not seasons:
        if not team_ids:
            return games, game_players

    normalized_team_ids = (
        {int(team_id) for team_id in team_ids or [] if int(team_id) > 0}
        or {resolve_team_id(team) for team in teams or []}
    )
    normalized_seasons = set(seasons or [])
    selected_game_ids = {
        game.game_id
        for game in games
        if (not normalized_seasons or game.season in normalized_seasons)
        and (not normalized_team_ids or game.team_id in normalized_team_ids)
    }
    if not selected_game_ids:
        raise ValueError("No games matched the requested RAWR scope")

    filtered_games = [game for game in games if game.game_id in selected_game_ids]
    filtered_game_players = [
        player for player in game_players if player.game_id in selected_game_ids
    ]
    return filtered_games, filtered_game_players


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
    progress = None
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
            f"{_require_team_id(game.opponent_team_id, game.game_id, 'opponent_team_id')}:{game.season}"
            for game in games
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


@dataclass(frozen=True)
class RawrSeasonCompletenessIssue:
    season: str
    reason: str


def list_expected_rawr_teams_for_season(season: str) -> list[str]:
    return list_expected_team_abbreviations_for_season(season)


def list_incomplete_rawr_seasons(
    *,
    seasons: list[str],
    season_type: str,
    player_metrics_db_path: Path,
) -> list[RawrSeasonCompletenessIssue]:
    cache_load_rows = list_cache_load_rows(
        player_metrics_db_path,
        season_type=season_type,
        seasons=seasons,
    )
    rows_by_season: dict[str, dict[str, object]] = {}
    for row in cache_load_rows:
        season_rows = rows_by_season.setdefault(
            row.season,
            {"teams": {}, "issues": set()},
        )
        season_rows["teams"][row.team] = row
        if row.expected_games_row_count is None or row.skipped_games_row_count is None:
            season_rows["issues"].add(
                "ERROR: MetaData incomplete/out of date. "
                f"Run `poetry run python scripts/cache_season_data.py {row.season}` "
                "or repopulate DB."
            )
        if (
            row.expected_games_row_count is not None
            and row.games_row_count != row.expected_games_row_count
        ):
            season_rows["issues"].add(
                f"partial team-season cache for {row.team} "
                f"({row.games_row_count}/{row.expected_games_row_count} games)"
            )
        if row.skipped_games_row_count:
            season_rows["issues"].add(
                f"skipped games present for {row.team} "
                f"({row.skipped_games_row_count} skipped)"
            )

    issues: list[RawrSeasonCompletenessIssue] = []
    for season in seasons:
        season_rows = rows_by_season.get(season)
        expected_teams = set(list_expected_rawr_teams_for_season(season))
        if season_rows is None:
            issues.append(
                RawrSeasonCompletenessIssue(
                    season=season,
                    reason="no cache load metadata found",
                )
            )
            continue
        missing_teams = sorted(expected_teams - set(season_rows["teams"]))
        if missing_teams:
            season_rows["issues"].add(
                f"missing team-seasons: {', '.join(missing_teams)}"
            )
        for reason in sorted(season_rows["issues"]):
            issues.append(RawrSeasonCompletenessIssue(season=season, reason=reason))
    return issues


def _require_team_id(team_id: int | None, game_id: str, field_name: str) -> int:
    if team_id is None or team_id <= 0:
        raise ValueError(
            f"Normalized cache-backed game {game_id!r} is missing {field_name}"
        )
    return team_id


def list_complete_rawr_seasons(
    *,
    seasons: list[str],
    season_type: str,
    player_metrics_db_path: Path,
) -> set[str]:
    cache_load_rows = list_cache_load_rows(
        player_metrics_db_path,
        season_type=season_type,
        seasons=seasons,
    )
    rows_by_season: dict[str, dict[str, object]] = {}
    for row in cache_load_rows:
        season_rows = rows_by_season.setdefault(
            row.season,
            {"teams": {}, "all_complete": True},
        )
        season_rows["teams"][row.team] = row
        if (
            row.expected_games_row_count is None
            or row.skipped_games_row_count is None
            or row.games_row_count != row.expected_games_row_count
            or row.skipped_games_row_count != 0
        ):
            season_rows["all_complete"] = False

    complete_seasons: set[str] = set()
    for season in seasons:
        season_rows = rows_by_season.get(season)
        if season_rows is None or not season_rows["all_complete"]:
            continue
        expected_teams = set(list_expected_rawr_teams_for_season(season))
        if set(season_rows["teams"]) != expected_teams:
            continue
        complete_seasons.add(season)
    return complete_seasons


def select_complete_rawr_scope_seasons(
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    team_ids: list[int] | None,
    season_type: str,
    player_metrics_db_path: Path,
) -> list[str]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        team_ids=team_ids,
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )
    candidate_seasons = sorted({team_season.season for team_season in team_seasons})
    if not candidate_seasons:
        return []
    complete_seasons = list_complete_rawr_seasons(
        seasons=candidate_seasons,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
    )
    return [season for season in candidate_seasons if season in complete_seasons]


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
    team_ids: list[int] | None = None,
    season_type: str,
    source_data_dir: Path,
    min_games: int,
    ridge_alpha: float,
    shrinkage_mode: str,
    shrinkage_strength: float,
    shrinkage_minute_scale: float,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> list[RawrPlayerSeasonRecord]:
    validate_filters(
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        team_ids=team_ids,
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )
    teams_by_season: dict[str, list[str]] = {}
    for team_season in team_seasons:
        teams_by_season.setdefault(team_season.season, []).append(team_season.team)
    complete_seasons = set(
        select_complete_rawr_scope_seasons(
            teams=teams,
            seasons=seasons,
            team_ids=team_ids,
            season_type=season_type,
            player_metrics_db_path=player_metrics_db_path,
        )
    )
    records: list[RawrPlayerSeasonRecord] = []

    for season in sorted(teams_by_season):
        if season not in complete_seasons:
            continue
        games, game_players = prepare_canonical_scope_records(
            teams=sorted(set(teams_by_season[season])),
            seasons=[season],
            season_type=season_type,
            source_data_dir=source_data_dir,
            player_metrics_db_path=player_metrics_db_path,
            include_opponents_for_team_scope=True,
            require_cached_only=True,
            log=lambda *_args, **_kwargs: None,
        )
        try:
            games, game_players = filter_rawr_scope(
                games,
                game_players,
                teams=sorted(set(teams_by_season[season])),
                seasons=[season],
                team_ids=team_ids,
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
                shrinkage_mode=shrinkage_mode,
                shrinkage_strength=shrinkage_strength,
                shrinkage_minute_scale=shrinkage_minute_scale,
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
        raise ValueError(
            "No complete cached seasons matched the requested RAWR scope"
        )
    print(
        f"[1/3] preparing RAWR inputs for {format_scope(args.team, args.season)}"
    )
    games, game_players = prepare_canonical_scope_records(
        teams=args.team,
        seasons=complete_seasons,
        season_type=args.season_type,
        source_data_dir=args.source_data_dir,
        player_metrics_db_path=getattr(
            args,
            "player_metrics_db_path",
            DEFAULT_PLAYER_METRICS_DB_PATH,
        ),
        include_opponents_for_team_scope=True,
        require_cached_only=True,
        log=lambda *_args, **_kwargs: None,
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
