from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nba_api.stats.static import teams as nba_teams

from wowy.nba.ingest import DEFAULT_NORMALIZED_GAMES_DIR


@dataclass(frozen=True, order=True)
class TeamSeasonScope:
    team: str
    season: str


def parse_team_season_filename(path: Path) -> TeamSeasonScope:
    team, separator, season = path.stem.partition("_")
    if not separator or not team or not season:
        raise ValueError(
            f"Unexpected team-season filename {path.name!r}. Expected TEAM_SEASON.csv."
        )
    return TeamSeasonScope(team=team.upper(), season=season)


def list_cached_team_seasons(
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
) -> list[TeamSeasonScope]:
    return sorted(
        parse_team_season_filename(path)
        for path in normalized_games_input_dir.glob("*.csv")
    )


def resolve_team_seasons(
    teams: list[str] | None,
    seasons: list[str] | None,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
) -> list[TeamSeasonScope]:
    normalized_teams = [team.upper() for team in teams] if teams else None
    cached_team_seasons = list_cached_team_seasons(normalized_games_input_dir)

    if seasons:
        target_teams = normalized_teams or sorted(
            team["abbreviation"] for team in nba_teams.get_teams()
        )
        return sorted(
            TeamSeasonScope(team=team, season=season)
            for season in seasons
            for team in target_teams
        )

    if normalized_teams:
        return [
            team_season
            for team_season in cached_team_seasons
            if team_season.team in normalized_teams
        ]

    return cached_team_seasons
