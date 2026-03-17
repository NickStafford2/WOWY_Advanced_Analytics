from __future__ import annotations

from pathlib import Path

from wowy.nba.team_seasons import TeamSeasonScope

DEFAULT_NORMALIZED_GAMES_DIR = Path("data/normalized/nba/games")
DEFAULT_NORMALIZED_GAME_PLAYERS_DIR = Path("data/normalized/nba/game_players")
DEFAULT_WOWY_GAMES_DIR = Path("data/raw/nba/team_games")


def season_type_slug(season_type: str) -> str:
    return season_type.lower().replace(" ", "_")


def team_season_filename(
    team_season: TeamSeasonScope,
    season_type: str = "Regular Season",
) -> str:
    return (
        f"{team_season.team}_{team_season.season}_{season_type_slug(season_type)}.csv"
    )


def legacy_regular_season_filename(team_season: TeamSeasonScope) -> str:
    return f"{team_season.team}_{team_season.season}.csv"


def normalized_games_path(
    team_season: TeamSeasonScope,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    season_type: str = "Regular Season",
) -> Path:
    return normalized_games_input_dir / team_season_filename(team_season, season_type)


def normalized_game_players_path(
    team_season: TeamSeasonScope,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    season_type: str = "Regular Season",
) -> Path:
    return normalized_game_players_input_dir / team_season_filename(
        team_season,
        season_type,
    )


def wowy_games_path(
    team_season: TeamSeasonScope,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    season_type: str = "Regular Season",
) -> Path:
    return wowy_output_dir / team_season_filename(team_season, season_type)


def candidate_paths(
    team_season: TeamSeasonScope,
    directory: Path,
    season_type: str = "Regular Season",
) -> list[Path]:
    explicit_path = directory / team_season_filename(team_season, season_type)
    if season_type != "Regular Season":
        return [explicit_path]
    return [explicit_path, directory / legacy_regular_season_filename(team_season)]


def resolve_existing_path(
    team_season: TeamSeasonScope,
    directory: Path,
    season_type: str = "Regular Season",
) -> Path | None:
    for path in candidate_paths(team_season, directory, season_type):
        if path.exists():
            return path
    return None
