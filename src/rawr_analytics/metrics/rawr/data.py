from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.game_cache.repository import list_cache_load_rows
from rawr_analytics.data.player_metrics_db.builders import build_rawr_player_season_metric_rows
from rawr_analytics.data.player_metrics_db.models import (
    PlayerSeasonMetricRow,
)
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics.rawr._observations import count_player_games
from rawr_analytics.shared.season import SeasonType

RAWR_METRIC = "rawr"
DEFAULT_RAWR_SHRINKAGE_MODE = "uniform"
DEFAULT_RAWR_SHRINKAGE_STRENGTH = 1.0
DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE = 48.0

__all__ = [
    "DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE",
    "DEFAULT_RAWR_SHRINKAGE_MODE",
    "DEFAULT_RAWR_SHRINKAGE_STRENGTH",
    "RAWR_METRIC",
    "RawrSeasonCompletenessIssue",
    "build_rawr_metric_rows",
    "count_player_games",
    "list_incomplete_rawr_seasons",
    "select_complete_rawr_scope_seasons",
]


@dataclass
class _SeasonIssueState:
    teams: dict[str, object]
    issues: set[str]


@dataclass
class _SeasonCompletionState:
    teams: dict[str, object]
    all_complete: bool = True


@dataclass(frozen=True)
class RawrSeasonCompletenessIssue:
    season: str
    reason: str


def list_incomplete_rawr_seasons(
    *,
    seasons: list[str],
    season_type: str,
) -> list[RawrSeasonCompletenessIssue]:
    normalized_season_type = SeasonType.parse(season_type)
    season_filter = set(seasons)
    cache_load_rows = [
        row
        for row in list_cache_load_rows()
        if row.season.id in season_filter and row.season.season_type == normalized_season_type
    ]
    rows_by_season: dict[str, _SeasonIssueState] = {}
    for row in cache_load_rows:
        season_rows = rows_by_season.setdefault(
            row.season.id,
            _SeasonIssueState(teams={}, issues=set()),
        )
        team_label = row.team.abbreviation(season=row.season)
        season_rows.teams[team_label] = row
        if row.expected_games_row_count is None or row.skipped_games_row_count is None:
            season_rows.issues.add(
                "ERROR: MetaData incomplete/out of date. "
                f"Run `poetry run python scripts/cache_season_data.py {row.season.id}` "
                "or repopulate DB."
            )
        if (
            row.expected_games_row_count is not None
            and row.games_row_count != row.expected_games_row_count
        ):
            season_rows.issues.add(
                f"partial team-season cache for {team_label} "
                f"({row.games_row_count}/{row.expected_games_row_count} games)"
            )
        if row.skipped_games_row_count:
            season_rows.issues.add(
                f"skipped games present for {team_label} ({row.skipped_games_row_count} skipped)"
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
        missing_teams = sorted(expected_teams - set(season_rows.teams))
        if missing_teams:
            season_rows.issues.add(f"missing team-seasons: {', '.join(missing_teams)}")
        for reason in sorted(season_rows.issues):
            issues.append(RawrSeasonCompletenessIssue(season=season, reason=reason))
    return issues


def list_complete_rawr_seasons(
    *,
    seasons: list[str],
    season_type: str,
) -> set[str]:
    normalized_season_type = SeasonType.parse(season_type)
    season_filter = set(seasons)
    cache_load_rows = [
        row
        for row in list_cache_load_rows()
        if row.season.id in season_filter and row.season.season_type == normalized_season_type
    ]
    rows_by_season: dict[str, _SeasonCompletionState] = {}
    for row in cache_load_rows:
        season_rows = rows_by_season.setdefault(
            row.season.id,
            _SeasonCompletionState(teams={}),
        )
        season_rows.teams[row.team.abbreviation(season=row.season)] = row
        if (
            row.expected_games_row_count is None
            or row.skipped_games_row_count is None
            or row.games_row_count != row.expected_games_row_count
            or row.skipped_games_row_count != 0
        ):
            season_rows.all_complete = False

    complete_seasons: set[str] = set()
    for season in seasons:
        season_rows = rows_by_season.get(season)
        if season_rows is None or not season_rows.all_complete:
            continue
        expected_teams = set(list_expected_rawr_teams_for_season(season))
        if set(season_rows.teams) != expected_teams:
            continue
        complete_seasons.add(season)
    return complete_seasons


def select_complete_rawr_scope_seasons(
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    team_ids: list[int] | None,
    season_type: str,
) -> list[str]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        team_ids=team_ids,
        season_type=season_type,
    )
    candidate_seasons = sorted({team_season.season.id for team_season in team_seasons})
    if not candidate_seasons:
        return []
    complete_seasons = list_complete_rawr_seasons(
        seasons=candidate_seasons,
        season_type=season_type,
    )
    return [season for season in candidate_seasons if season in complete_seasons]


def build_rawr_metric_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    teams: list[str] | None,
    team_ids: list[int] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    from rawr_analytics.metrics.rawr.records import prepare_rawr_player_season_records

    records = prepare_rawr_player_season_records(
        teams=teams,
        team_ids=team_ids,
        seasons=None,
        season_type=season_type,
        min_games=1,
        ridge_alpha=rawr_ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return build_rawr_player_season_metric_rows(
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type,
        records=records,
    )
