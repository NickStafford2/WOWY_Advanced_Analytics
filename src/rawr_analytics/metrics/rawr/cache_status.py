from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.shared.season import Season, normalize_seasons
from rawr_analytics.shared.team import Team


@dataclass
class _SeasonCacheSummary:
    team_labels: set[str]
    incomplete_metadata_teams: set[str]
    partial_teams: dict[str, tuple[int, int]]
    skipped_teams: dict[str, int]


def list_complete_rawr_seasons(
    *,
    seasons: list[Season],
) -> set[Season]:
    assert seasons, "RAWR completeness requires explicit seasons"
    summaries = _summarize_rawr_cache_seasons(seasons=seasons)
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
    seasons: list[Season],
) -> list[str]:
    assert seasons, "RAWR completeness warnings require explicit seasons"
    requested_seasons = normalize_seasons(seasons)
    assert requested_seasons is not None
    summaries = _summarize_rawr_cache_seasons(seasons=requested_seasons)
    warnings: list[str] = []
    for season in requested_seasons:
        summary = summaries.get(season)
        if summary is None:
            warnings.append(f"{season.id}: no cache load metadata found")
            continue
        warnings.extend(_build_rawr_warning_messages(season=season, summary=summary))
    return warnings


def _summarize_rawr_cache_seasons(
    *,
    seasons: list[Season],
) -> dict[Season, _SeasonCacheSummary]:
    season_filter = set(seasons)
    summaries: dict[Season, _SeasonCacheSummary] = {}
    snapshot = load_game_cache_snapshot()
    for entry in snapshot.entries:
        scope = entry.scope
        if scope.season not in season_filter:
            continue
        summary = summaries.setdefault(
            scope.season,
            _SeasonCacheSummary(
                team_labels=set(),
                incomplete_metadata_teams=set(),
                partial_teams={},
                skipped_teams={},
            ),
        )
        team_label = scope.team.abbreviation(season=scope.season)
        summary.team_labels.add(team_label)
        if entry.expected_games_count is None or entry.skipped_games_count is None:
            summary.incomplete_metadata_teams.add(team_label)
            continue
        if entry.games_count != entry.expected_games_count:
            summary.partial_teams[team_label] = (
                entry.games_count,
                entry.expected_games_count,
            )
        if entry.skipped_games_count != 0:
            summary.skipped_teams[team_label] = entry.skipped_games_count
    return summaries


def _is_complete_rawr_summary(*, season: Season, summary: _SeasonCacheSummary) -> bool:
    return (
        (
            season.is_playoffs()
            or summary.team_labels == set(_list_expected_rawr_teams_for_season(season))
        )
        and not summary.incomplete_metadata_teams
        and not summary.partial_teams
        and not summary.skipped_teams
    )


def _build_rawr_warning_messages(*, season: Season, summary: _SeasonCacheSummary) -> list[str]:
    warnings: list[str] = []
    if not season.is_playoffs():
        missing_teams = sorted(
            set(_list_expected_rawr_teams_for_season(season)) - summary.team_labels
        )
        if missing_teams:
            warnings.append(f"{season.id}: missing team-seasons: {', '.join(missing_teams)}")
    warnings.extend(
        f"{season.id}: incomplete cache metadata for {team_label}"
        for team_label in sorted(summary.incomplete_metadata_teams)
    )
    warnings.extend(
        f"{season.id}: partial team-season cache for {team_label} ({games}/{expected} games)"
        for team_label, (games, expected) in sorted(summary.partial_teams.items())
    )
    warnings.extend(
        f"{season.id}: skipped games present for {team_label} ({skipped} skipped)"
        for team_label, skipped in sorted(summary.skipped_teams.items())
    )
    return warnings


def _list_expected_rawr_teams_for_season(season: Season) -> list[str]:
    return [team.abbreviation(season=season) for team in Team.all_active_in_season(season)]
