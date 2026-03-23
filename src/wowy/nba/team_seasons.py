from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nba_api.stats.static import teams as nba_teams

from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.team_identity import resolve_team_id


@dataclass(frozen=True, order=True)
class TeamSeasonScope:
    team: str
    season: str
    team_id: int | None = None


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
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    season_type: str | None = None,
) -> list[TeamSeasonScope]:
    normalized_team_ids = (
        {resolve_team_id(team) for team in teams}
        if teams
        else None
    )
    normalized_seasons = (
        [canonicalize_season_string(season) for season in seasons]
        if seasons
        else None
    )
    if season_type is not None:
        season_type = canonicalize_season_type(season_type)
    cached_team_seasons = list_cached_team_seasons(
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )

    if normalized_seasons:
        if normalized_team_ids is not None:
            target_teams = [
                team_season
                for team_season in cached_team_seasons
                if team_season.team_id in normalized_team_ids
            ]
            if not target_teams:
                target_teams = [
                    TeamSeasonScope(
                        team=team.upper(),
                        team_id=resolve_team_id(team),
                        season=normalized_seasons[0],
                    )
                    for team in teams or []
                ]
        else:
            target_teams = cached_team_seasons or [
                TeamSeasonScope(
                    team=str(team["abbreviation"]),
                    team_id=int(team["id"]),
                    season=normalized_seasons[0],
                )
                for team in nba_teams.get_teams()
            ]
        return sorted(
            TeamSeasonScope(
                team=team_season.team,
                team_id=team_season.team_id,
                season=season,
            )
            for season in normalized_seasons
            for team_season in target_teams
        )

    if normalized_team_ids:
        return [
            team_season
            for team_season in cached_team_seasons
            if team_season.team_id in normalized_team_ids
        ]

    return cached_team_seasons
