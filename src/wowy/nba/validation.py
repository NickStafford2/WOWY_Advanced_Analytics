from __future__ import annotations

from pathlib import Path

from wowy.apps.wowy.derive import derive_wowy_games
from wowy.data.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)
from wowy.data.wowy_io import load_games_from_csv
from wowy.nba.seasons import canonicalize_season_string


def validate_team_season_files(
    normalized_games_path: Path,
    normalized_game_players_path: Path,
    wowy_path: Path,
) -> str:
    try:
        games = load_normalized_games_from_csv(normalized_games_path)
        game_players = load_normalized_game_players_from_csv(normalized_game_players_path)
        wowy_games = load_games_from_csv(wowy_path)
    except (OSError, ValueError):
        return "corrupt"

    game_keys = [(game.game_id, game.team) for game in games]
    if len(set(game_keys)) != len(game_keys):
        return "dup_games"

    player_keys = {(player.game_id, player.team) for player in game_players}
    if set(game_keys) - player_keys:
        return "missing_players"

    try:
        derived_wowy_games = derive_wowy_games(games, game_players)
    except ValueError:
        return "invalid_players"

    derived_by_key = {(game.game_id, game.team): game for game in derived_wowy_games}
    wowy_by_key = {(game.game_id, game.team): game for game in wowy_games}
    if set(derived_by_key) != set(wowy_by_key):
        return "wowy_keys"

    for key, derived_game in derived_by_key.items():
        wowy_game = wowy_by_key[key]
        if (
            derived_game.season != wowy_game.season
            or derived_game.margin != wowy_game.margin
            or derived_game.players != wowy_game.players
        ):
            return "wowy_data"

    return "ok"


def validate_team_season_consistency(
    team: str,
    season: str,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
) -> str:
    season = canonicalize_season_string(season)
    return validate_team_season_files(
        normalized_games_path=normalized_games_input_dir / f"{team}_{season}.csv",
        normalized_game_players_path=(
            normalized_game_players_input_dir / f"{team}_{season}.csv"
        ),
        wowy_path=wowy_output_dir / f"{team}_{season}.csv",
    )
