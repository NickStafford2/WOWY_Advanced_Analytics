from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.game_cache.repository import list_cache_load_rows
from rawr_analytics.data.player_metrics_db.builders import build_rawr_player_season_metric_rows
from rawr_analytics.data.player_metrics_db.models import (
    PlayerSeasonMetricRow,
)
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics.rawr._observations import count_player_games
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

DEFAULT_RAWR_SHRINKAGE_MODE = "uniform"
DEFAULT_RAWR_SHRINKAGE_STRENGTH = 1.0
DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE = 48.0

__all__ = [
    "DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE",
    "DEFAULT_RAWR_SHRINKAGE_MODE",
    "DEFAULT_RAWR_SHRINKAGE_STRENGTH",
    "RawrSeasonCompletenessIssue",
    "build_rawr_metric_rows",
    "count_player_games",
    "list_complete_rawr_seasons",
    "list_incomplete_rawr_seasons",
    "list_incomplete_rawr_season_warnings",
    "list_expected_rawr_teams_for_season",
    "select_complete_rawr_scope_seasons",
]


@dataclass
class _SeasonCacheSummary:
    team_labels: set[str]
    incomplete_metadata_teams: set[str]
    partial_teams: dict[str, tuple[int, int]]
    skipped_teams: dict[str, int]


@dataclass(frozen=True)
class RawrSeasonCompletenessIssue:
    season: str
    reason: str


def list_expected_rawr_teams_for_season(season: str) -> list[str]:
    resolved_season = Season(season, "Regular Season")
    return [
        team.abbreviation(season=resolved_season)
        for team in Team.all_active_in_season(resolved_season)
    ]


def list_incomplete_rawr_seasons(
    *,
    seasons: list[str],
    season_type: str,
) -> list[RawrSeasonCompletenessIssue]:
    summaries = _summarize_rawr_cache_seasons(seasons=seasons, season_type=season_type)
    issues: list[RawrSeasonCompletenessIssue] = []
    for season in seasons:
        summary = summaries.get(season)
        if summary is None:
            issues.append(
                RawrSeasonCompletenessIssue(
                    season=season,
                    reason="no cache load metadata found",
                )
            )
            continue
        for reason in _build_rawr_issue_reasons(season=season, summary=summary):
            issues.append(RawrSeasonCompletenessIssue(season=season, reason=reason))
    return issues


def list_complete_rawr_seasons(
    *,
    seasons: list[str],
    season_type: str,
) -> set[str]:
    summaries = _summarize_rawr_cache_seasons(seasons=seasons, season_type=season_type)
    return {
        season
        for season in seasons
        if season in summaries
        and _is_complete_rawr_summary(
            season=season,
            summary=summaries[season],
        )
    }


def list_incomplete_rawr_season_warnings(
    *,
    seasons: list[str] | None = None,
    season_type: str,
) -> list[str]:
    requested_seasons = seasons or _list_cached_rawr_seasons_for_type(season_type)
    summaries = _summarize_rawr_cache_seasons(
        seasons=requested_seasons,
        season_type=season_type,
    )
    warnings: list[str] = []
    for season in requested_seasons:
        summary = summaries.get(season)
        if summary is None:
            warnings.append(f"{season}: no cache load metadata found")
            continue
        warnings.extend(_build_rawr_warning_messages(season=season, summary=summary))
    return warnings


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


def _summarize_rawr_cache_seasons(
    *,
    seasons: list[str],
    season_type: str,
) -> dict[str, _SeasonCacheSummary]:
    normalized_season_type = SeasonType.parse(season_type)
    season_filter = set(seasons)
    summaries: dict[str, _SeasonCacheSummary] = {}
    for row in list_cache_load_rows():
        if row.season.id not in season_filter or row.season.season_type != normalized_season_type:
            continue
        summary = summaries.setdefault(
            row.season.id,
            _SeasonCacheSummary(
                team_labels=set(),
                incomplete_metadata_teams=set(),
                partial_teams={},
                skipped_teams={},
            ),
        )
        team_label = row.team.abbreviation(season=row.season)
        summary.team_labels.add(team_label)
        if row.expected_games_row_count is None or row.skipped_games_row_count is None:
            summary.incomplete_metadata_teams.add(team_label)
            continue
        if row.games_row_count != row.expected_games_row_count:
            summary.partial_teams[team_label] = (
                row.games_row_count,
                row.expected_games_row_count,
            )
        if row.skipped_games_row_count != 0:
            summary.skipped_teams[team_label] = row.skipped_games_row_count
    return summaries


def _is_complete_rawr_summary(*, season: str, summary: _SeasonCacheSummary) -> bool:
    return (
        summary.team_labels == set(list_expected_rawr_teams_for_season(season))
        and not summary.incomplete_metadata_teams
        and not summary.partial_teams
        and not summary.skipped_teams
    )


def _build_rawr_issue_reasons(*, season: str, summary: _SeasonCacheSummary) -> list[str]:
    reasons: list[str] = []
    missing_teams = sorted(set(list_expected_rawr_teams_for_season(season)) - summary.team_labels)
    if missing_teams:
        reasons.append(f"missing team-seasons: {', '.join(missing_teams)}")
    if summary.incomplete_metadata_teams:
        reasons.append(
            "ERROR: MetaData incomplete/out of date. "
            f"Run `poetry run python scripts/cache_season_data.py {season}` "
            "or repopulate DB."
        )
    reasons.extend(
        f"partial team-season cache for {team_label} ({games}/{expected} games)"
        for team_label, (games, expected) in sorted(summary.partial_teams.items())
    )
    reasons.extend(
        f"skipped games present for {team_label} ({skipped} skipped)"
        for team_label, skipped in sorted(summary.skipped_teams.items())
    )
    return sorted(reasons)


def _build_rawr_warning_messages(*, season: str, summary: _SeasonCacheSummary) -> list[str]:
    warnings: list[str] = []
    missing_teams = sorted(set(list_expected_rawr_teams_for_season(season)) - summary.team_labels)
    if missing_teams:
        warnings.append(f"{season}: missing team-seasons: {', '.join(missing_teams)}")
    warnings.extend(
        f"{season}: incomplete cache metadata for {team_label}"
        for team_label in sorted(summary.incomplete_metadata_teams)
    )
    warnings.extend(
        f"{season}: partial team-season cache for {team_label} ({games}/{expected} games)"
        for team_label, (games, expected) in sorted(summary.partial_teams.items())
    )
    warnings.extend(
        f"{season}: skipped games present for {team_label} ({skipped} skipped)"
        for team_label, skipped in sorted(summary.skipped_teams.items())
    )
    return warnings


def _list_cached_rawr_seasons_for_type(season_type: str) -> list[str]:
    normalized_season_type = SeasonType.parse(season_type)
    return sorted(
        {
            row.season.id
            for row in list_cache_load_rows()
            if row.season.season_type == normalized_season_type
        }
    )
