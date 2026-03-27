from __future__ import annotations

from rawr_analytics.data.game_cache.repository import (
    list_cached_team_seasons,
    load_normalized_scope_records_from_db,
)
from rawr_analytics.data.scopes import TeamSeasonScope
from rawr_analytics.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.nba.seasons import canonicalize_season_year_string
from rawr_analytics.nba.team_identity import (
    list_expected_team_abbreviations_for_season,
    resolve_team_id,
)


def resolve_team_seasons(
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    season_type: str | None = None,
) -> list[TeamSeasonScope]:
    normalized_seasons = (
        [canonicalize_season_year_string(season) for season in seasons] if seasons else None
    )
    normalized_team_ids = (
        sorted({int(team_id) for team_id in team_ids if int(team_id) > 0}) if team_ids else None
    )
    if season_type is not None:
        season_type = canonicalize_season_type(season_type)
    cached_team_seasons = list_cached_team_seasons(season_type=season_type)
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
        resolved: list[TeamSeasonScope] = []
        for season in normalized_seasons:
            season_rows = [
                team_season for team_season in cached_team_seasons if team_season.season == season
            ]
            if season_rows:
                resolved.extend(season_rows)
                continue
            resolved.extend(
                TeamSeasonScope(
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

    return cached_team_seasons


def load_normalized_scope_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    season_type: str = "Regular Season",
    include_opponents_for_team_scope: bool = True,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    team_seasons = resolve_team_seasons(
        seasons,
        team_ids=team_ids,
        season_type=season_type,
    )
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")

    requested_team_seasons = list(team_seasons)

    if (teams or team_ids) and include_opponents_for_team_scope:
        opponent_team_seasons = {
            TeamSeasonScope(
                team_id=game.opponent_team_id,
                season=game.season,
            )
            for game in _load_normalized_games_from_db_for_scope(
                requested_team_seasons,
                season_type=season_type,
            )
        }
        for team_season in sorted(opponent_team_seasons):
            if team_season in requested_team_seasons:
                continue
            team_seasons.append(team_season)

    return load_normalized_scope_records_from_db(
        team_seasons=team_seasons,
        season_type=season_type,
    )


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
            resolved.append(
                TeamSeasonScope(
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
                    team_id=team_id,
                    season=season,
                )
            )
    return sorted(resolved)


def _load_normalized_games_from_db_for_scope(
    team_seasons: list[TeamSeasonScope],
    *,
    season_type: str,
) -> list[NormalizedGameRecord]:
    games, _ = load_normalized_scope_records_from_db(
        team_seasons=team_seasons,
        season_type=season_type,
    )
    return games
