from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import date

from wowy.apps.wowy.derive import derive_wowy_games
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type


_TEAM_ABBREVIATION_PATTERN = re.compile(r"^[A-Z]{3}$")
_GAME_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_normalized_cache_batch(
    *,
    team: str,
    season: str,
    season_type: str,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> None:
    normalized_team = _canonical_team_abbreviation(team)
    normalized_season = canonicalize_season_string(season)
    normalized_season_type = canonicalize_season_type(season_type)

    game_keys: set[tuple[str, str]] = set()
    players_by_game_key: dict[tuple[str, str], list[NormalizedGamePlayerRecord]] = (
        defaultdict(list)
    )

    for game in games:
        _validate_normalized_game(
            game,
            expected_team=normalized_team,
            expected_season=normalized_season,
            expected_season_type=normalized_season_type,
        )
        game_key = (game.game_id, game.team)
        if game_key in game_keys:
            raise ValueError(f"Duplicate normalized game row for {game_key!r}")
        game_keys.add(game_key)

    player_keys: set[tuple[str, str, int]] = set()
    for player in game_players:
        _validate_normalized_game_player(player, expected_team=normalized_team)
        player_key = (player.game_id, player.team, player.player_id)
        if player_key in player_keys:
            raise ValueError(f"Duplicate normalized player row for {player_key!r}")
        player_keys.add(player_key)
        players_by_game_key[(player.game_id, player.team)].append(player)

    if set(players_by_game_key) != game_keys:
        missing_players = sorted(game_keys - set(players_by_game_key))
        extra_players = sorted(set(players_by_game_key) - game_keys)
        raise ValueError(
            "Normalized game/player keys do not match: "
            f"missing_players={missing_players} extra_players={extra_players}"
        )

    for game_key, players in players_by_game_key.items():
        appeared_players = [player for player in players if player.appeared]
        if len(appeared_players) < 5:
            raise ValueError(
                f"Expected at least 5 appeared players for {game_key!r}; "
                f"found {len(appeared_players)}"
            )
        if len(players) > 25:
            raise ValueError(
                f"Expected at most 25 player rows for {game_key!r}; found {len(players)}"
            )

        total_minutes = sum(player.minutes or 0.0 for player in appeared_players)
        if total_minutes < 235.0 or total_minutes > 450.0:
            raise ValueError(
                f"Implausible total appeared minutes for {game_key!r}: {total_minutes}"
            )

    consistency = validate_team_season_records(
        games=games,
        game_players=game_players,
        wowy_games=derive_wowy_games(games, game_players),
    )
    if consistency != "ok":
        raise ValueError(f"Normalized cache consistency check failed: {consistency}")


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


def _validate_normalized_game(
    game: NormalizedGameRecord,
    *,
    expected_team: str,
    expected_season: str,
    expected_season_type: str,
) -> None:
    if not game.game_id.strip():
        raise ValueError("Normalized game_id must not be empty")
    if canonicalize_season_string(game.season) != expected_season:
        raise ValueError(
            f"Normalized game {game.game_id!r} has season {game.season!r}; "
            f"expected {expected_season!r}"
        )
    if canonicalize_season_type(game.season_type) != expected_season_type:
        raise ValueError(
            f"Normalized game {game.game_id!r} has season type {game.season_type!r}; "
            f"expected {expected_season_type!r}"
        )
    if _canonical_team_abbreviation(game.team) != expected_team:
        raise ValueError(
            f"Normalized game {game.game_id!r} has team {game.team!r}; "
            f"expected {expected_team!r}"
        )
    if _canonical_team_abbreviation(game.opponent) == expected_team:
        raise ValueError(
            f"Normalized game {game.game_id!r} must not use the same team and opponent"
        )
    if not _GAME_DATE_PATTERN.fullmatch(game.game_date):
        raise ValueError(
            f"Normalized game {game.game_id!r} has invalid game date {game.game_date!r}"
        )
    parsed_date = date.fromisoformat(game.game_date)
    start_year = int(expected_season[:4])
    if parsed_date.year not in {start_year, start_year + 1}:
        raise ValueError(
            f"Normalized game {game.game_id!r} date {game.game_date!r} "
            f"falls outside season {expected_season!r}"
        )
    if not math.isfinite(game.margin):
        raise ValueError(f"Normalized game {game.game_id!r} has non-finite margin")
    if not game.source.strip():
        raise ValueError(
            f"Normalized game {game.game_id!r} must have a non-empty source"
        )


def _validate_normalized_game_player(
    player: NormalizedGamePlayerRecord,
    *,
    expected_team: str,
) -> None:
    source_path = (
        f"data/source/nba/boxscores/{player.game_id}_boxscoretraditionalv2.json"
    )
    player_ref = (
        f"game {player.game_id!r} player_id={player.player_id!r} "
        f"player_name={player.player_name!r} source_path={source_path!r}"
    )
    if not player.game_id.strip():
        raise ValueError("Normalized player game_id must not be empty")
    if _canonical_team_abbreviation(player.team) != expected_team:
        raise ValueError(
            f"Normalized player row for game {player.game_id!r} has team {player.team!r}; "
            f"expected {expected_team!r}"
        )
    if player.player_id <= 0:
        raise ValueError(
            f"Normalized player row for {player_ref} has invalid player_id "
            f"{player.player_id!r}"
        )
    if not player.player_name.strip():
        raise ValueError(
            f"Normalized player row for {player_ref} must have a player name"
        )

    minutes = player.minutes
    if minutes is not None:
        if not math.isfinite(minutes) or minutes < 0.0:
            raise ValueError(
                f"Normalized player row for {player_ref} has invalid minutes "
                f"{minutes!r}"
            )
        if minutes > 80.0:
            raise ValueError(
                f"Normalized player row for {player_ref} has implausible minutes "
                f"{minutes!r}"
            )

    if player.appeared:
        if minutes is None or minutes <= 0.0:
            raise ValueError(
                f"Appeared player {player.player_id!r} in game {player.game_id!r} "
                "must have positive minutes"
            )
    elif minutes not in {None, 0.0}:
        raise ValueError(
            f"Did-not-appear player {player.player_id!r} in game {player.game_id!r} "
            "must have zero or null minutes"
        )


def _canonical_team_abbreviation(value: str) -> str:
    team = value.strip().upper()
    if not _TEAM_ABBREVIATION_PATTERN.fullmatch(team):
        raise ValueError(f"Invalid team abbreviation {value!r}")
    return team
