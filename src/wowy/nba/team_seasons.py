from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nba_api.stats.static import teams as nba_teams

from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.seasons import canonicalize_season_string

DEFAULT_NORMALIZED_GAMES_DIR = Path("data/normalized/nba/games")


@dataclass(frozen=True, order=True)
class TeamSeasonScope:
    team: str
    season: str


def parse_team_season_filename(path: Path) -> TeamSeasonScope:
    parts = path.stem.split("_")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"Unexpected team-season filename {path.name!r}. Expected TEAM_SEASON.csv."
        )
    team, season = parts[0], parts[1]
    canonical_season = canonicalize_season_string(season)
    if season != canonical_season:
        raise ValueError(
            f"Non-canonical season key in filename {path.name!r}. Expected {canonical_season!r}."
        )
    return TeamSeasonScope(team=team.upper(), season=canonical_season)


def list_cached_team_seasons(
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    *,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    season_type: str | None = None,
) -> list[TeamSeasonScope]:
    csv_team_seasons = {
        parse_team_season_filename(path)
        for path in normalized_games_input_dir.glob("*.csv")
    }
    if csv_team_seasons:
        return sorted(csv_team_seasons)
    from wowy.data.game_cache_db import list_cached_team_seasons_from_db

    return list_cached_team_seasons_from_db(
        player_metrics_db_path,
        season_type=season_type,
    )


def resolve_team_seasons(
    teams: list[str] | None,
    seasons: list[str] | None,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    *,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    season_type: str | None = None,
) -> list[TeamSeasonScope]:
    normalized_teams = [team.upper() for team in teams] if teams else None
    normalized_seasons = (
        [canonicalize_season_string(season) for season in seasons]
        if seasons
        else None
    )
    cached_team_seasons = list_cached_team_seasons(
        normalized_games_input_dir,
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )

    if normalized_seasons:
        target_teams = normalized_teams or sorted(
            team["abbreviation"] for team in nba_teams.get_teams()
        )
        return sorted(
            TeamSeasonScope(team=team, season=season)
            for season in normalized_seasons
            for team in target_teams
        )

    if normalized_teams:
        return [
            team_season
            for team_season in cached_team_seasons
            if team_season.team in normalized_teams
        ]

    return cached_team_seasons
