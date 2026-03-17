from __future__ import annotations

from wowy.apps.wowy.derive import derive_wowy_games
def validate_team_season_records(
    games,
    game_players,
    wowy_games,
) -> str:
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
