from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from wowy.apps.rawr.models import RawrObservation
from wowy.apps.rawr.models import RawrPlayerSeasonRecord
from wowy.apps.rawr.models import RawrResult
from wowy.apps.rawr.models import RawrPlayerEstimate
from wowy.data.game_cache import list_cache_load_rows
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.data.player_metrics_db import PlayerSeasonMetricRow
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.prepare import prepare_canonical_scope_records
from wowy.nba.team_identity import list_expected_team_abbreviations_for_season, resolve_team_id
from wowy.nba.team_seasons import resolve_team_seasons
from wowy.shared.minutes import passes_minute_filters

LINEUP_WEIGHT_SUM = 5.0
RAWR_METRIC = "rawr"
DEFAULT_RAWR_SHRINKAGE_MODE = "uniform"
DEFAULT_RAWR_SHRINKAGE_STRENGTH = 1.0
DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE = 48.0


def build_rawr_observations(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[list[RawrObservation], dict[int, str]]:
    player_minutes_by_game_team: dict[tuple[str, int], dict[int, float]] = defaultdict(dict)
    player_names: dict[int, str] = {}

    for player in game_players:
        player_names[player.player_id] = player.player_name
        if not player.appeared:
            continue
        minutes = player.minutes
        if minutes is None or minutes <= 0.0:
            raise ValueError(
                f"Missing positive minutes for appeared player {player.player_id!r} "
                f"in game {player.game_id!r} and team {player.team!r}"
            )
        player_minutes_by_game_team[(player.game_id, player.identity_team)][player.player_id] = (
            minutes
        )

    games_by_id: dict[str, list[NormalizedGameRecord]] = defaultdict(list)
    for game in games:
        games_by_id[game.game_id].append(game)

    observations: list[RawrObservation] = []
    for game_id, game_rows in sorted(games_by_id.items()):
        if len(game_rows) != 2:
            raise ValueError(
                f"Expected exactly two team rows for game {game_id!r}, found {len(game_rows)}"
            )

        home_games = [game for game in game_rows if game.is_home]
        away_games = [game for game in game_rows if not game.is_home]
        if len(home_games) != 1 or len(away_games) != 1:
            raise ValueError(
                f"Expected one home row and one away row for game {game_id!r}"
            )

        home_game = home_games[0]
        away_game = away_games[0]
        home_player_minutes = player_minutes_by_game_team.get(
            (game_id, home_game.identity_team), {}
        )
        away_player_minutes = player_minutes_by_game_team.get(
            (game_id, away_game.identity_team), {}
        )
        if not home_player_minutes:
            raise ValueError(
                f"No appeared players found for game {game_id!r} and team {home_game.team!r}"
            )
        if not away_player_minutes:
            raise ValueError(
                f"No appeared players found for game {game_id!r} and team {away_game.team!r}"
            )

        player_weights: dict[int, float] = {}
        for player_id, weight in build_minute_weights(home_player_minutes).items():
            player_weights[player_id] = weight
        for player_id, weight in build_minute_weights(away_player_minutes).items():
            player_weights[player_id] = -weight

        observations.append(
            RawrObservation(
                game_id=game_id,
                season=home_game.season,
                game_date=home_game.game_date,
                home_team=home_game.team,
                away_team=away_game.team,
                margin=home_game.margin,
                player_weights=player_weights,
                player_minutes=home_player_minutes | away_player_minutes,
                home_team_id=home_game.team_id,
                away_team_id=away_game.team_id,
            )
        )

    return observations, player_names


def build_minute_weights(player_minutes: dict[int, float]) -> dict[int, float]:
    total_minutes = sum(player_minutes.values())
    if total_minutes <= 0.0:
        raise ValueError("Expected positive total team minutes for RAWR observation")

    return {
        player_id: (minutes / total_minutes) * LINEUP_WEIGHT_SUM
        for player_id, minutes in player_minutes.items()
    }


def count_player_games(observations: list[RawrObservation]) -> dict[int, int]:
    games_by_player: dict[int, int] = defaultdict(int)
    for observation in observations:
        for player_id in observation.player_weights:
            games_by_player[player_id] += 1
    return dict(games_by_player)


def count_player_season_games(
    observations: list[RawrObservation],
) -> dict[tuple[str, int], int]:
    games_by_player_season: dict[tuple[str, int], int] = defaultdict(int)
    for observation in observations:
        for player_id in observation.player_weights:
            games_by_player_season[(observation.season, player_id)] += 1
    return dict(games_by_player_season)


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
    del source_data_dir
    from wowy.apps.rawr.analysis import fit_player_rawr

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
            player_metrics_db_path=player_metrics_db_path,
            include_opponents_for_team_scope=True,
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


def build_rawr_metric_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    source_data_dir: Path,
    db_path: Path,
    teams: list[str] | None,
    team_ids: list[int] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    records = prepare_rawr_player_season_records(
        teams=teams,
        team_ids=team_ids,
        seasons=None,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
        min_games=1,
        ridge_alpha=rawr_ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return [
        PlayerSeasonMetricRow(
            metric=RAWR_METRIC,
            metric_label="RAWR",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            season=record.season,
            player_id=record.player_id,
            player_name=record.player_name,
            value=record.coefficient,
            sample_size=record.games,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
            details={
                "games": record.games,
            },
        )
        for record in records
    ]
