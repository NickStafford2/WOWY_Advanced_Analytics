from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.team_history import resolve_team_history_entry_from_id
from wowy.nba.team_identity import (
    list_expected_team_abbreviations_for_season,
    resolve_team_id,
)


@dataclass(frozen=True, order=True)
class TeamSeasonScope:
    team: str
    season: str
    team_id: int


def list_cached_team_seasons(
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    season_type: str | None = None,
) -> list[TeamSeasonScope]:
    from wowy.data.game_cache_db import list_cached_team_seasons_from_db

    if season_type is not None:
        season_type = canonicalize_season_type(season_type)
    return list_cached_team_seasons_from_db(
        player_metrics_db_path,
        season_type=season_type,
    )


def resolve_team_seasons(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    season_type: str | None = None,
) -> list[TeamSeasonScope]:
    normalized_seasons = (
        [canonicalize_season_string(season) for season in seasons]
        if seasons
        else None
    )
    normalized_team_ids = (
        sorted({int(team_id) for team_id in team_ids if int(team_id) > 0})
        if team_ids
        else None
    )
    if season_type is not None:
        season_type = canonicalize_season_type(season_type)
    cached_team_seasons = list_cached_team_seasons(
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )
    cached_team_seasons_by_key = {
        (team_season.team_id, team_season.season): team_season
        for team_season in cached_team_seasons
    }

    if normalized_seasons:
        if normalized_team_ids is not None:
            return _resolve_team_id_scoped_seasons(
                team_ids=normalized_team_ids,
                seasons=normalized_seasons,
                cached_team_seasons_by_key=cached_team_seasons_by_key,
            )
        if teams:
            return _resolve_team_lookup_scoped_seasons(
                teams=teams,
                seasons=normalized_seasons,
                cached_team_seasons_by_key=cached_team_seasons_by_key,
            )
        resolved: list[TeamSeasonScope] = []
        for season in normalized_seasons:
            season_rows = [
                team_season
                for team_season in cached_team_seasons
                if team_season.season == season
            ]
            if season_rows:
                resolved.extend(season_rows)
                continue
            resolved.extend(
                TeamSeasonScope(
                    team=team,
                    team_id=resolve_team_id(team, season=season),
                    season=season,
                )
                for team in list_expected_team_abbreviations_for_season(season)
            )
        return sorted(resolved)

    if normalized_team_ids is not None:
        return [
            team_season
            for team_season in cached_team_seasons
            if team_season.team_id in normalized_team_ids
        ]

    if teams:
        normalized_lookup_team_ids = {resolve_team_id(team) for team in teams}
        return [
            team_season
            for team_season in cached_team_seasons
            if team_season.team_id in normalized_lookup_team_ids
        ]

    return cached_team_seasons


def _resolve_team_id_scoped_seasons(
    *,
    team_ids: list[int],
    seasons: list[str],
    cached_team_seasons_by_key: dict[tuple[int, str], TeamSeasonScope],
) -> list[TeamSeasonScope]:
    resolved: list[TeamSeasonScope] = []
    for season in seasons:
        for team_id in team_ids:
            cached_team_season = cached_team_seasons_by_key.get((team_id, season))
            if cached_team_season is not None:
                resolved.append(cached_team_season)
                continue
            entry = resolve_team_history_entry_from_id(team_id, season=season)
            resolved.append(
                TeamSeasonScope(
                    team=entry.abbreviation,
                    team_id=team_id,
                    season=season,
                )
            )
    return sorted(resolved)


def _resolve_team_lookup_scoped_seasons(
    *,
    teams: list[str],
    seasons: list[str],
    cached_team_seasons_by_key: dict[tuple[int, str], TeamSeasonScope],
) -> list[TeamSeasonScope]:
    resolved: list[TeamSeasonScope] = []
    for season in seasons:
        for team in teams:
            team_id = resolve_team_id(team, season=season)
            cached_team_season = cached_team_seasons_by_key.get((team_id, season))
            if cached_team_season is not None:
                resolved.append(cached_team_season)
                continue
            resolved.append(
                TeamSeasonScope(
                    team=resolve_team_history_entry_from_id(team_id, season=season).abbreviation,
                    team_id=team_id,
                    season=season,
                )
            )
    return sorted(resolved)
