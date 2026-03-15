from __future__ import annotations

from pathlib import Path

from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.nba.team_seasons import TeamSeasonScope


def normalized_games_path(
    team_season: TeamSeasonScope,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
) -> Path:
    return normalized_games_input_dir / f"{team_season.team}_{team_season.season}.csv"


def normalized_game_players_path(
    team_season: TeamSeasonScope,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
) -> Path:
    return (
        normalized_game_players_input_dir
        / f"{team_season.team}_{team_season.season}.csv"
    )


def wowy_games_path(
    team_season: TeamSeasonScope,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
) -> Path:
    return wowy_output_dir / f"{team_season.team}_{team_season.season}.csv"
