from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import date

from rawr_analytics.shared.game import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
)
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_GAME_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_normalized_team_season_batch(batch: NormalizedTeamSeasonBatch) -> None:
    game_keys: set[tuple[str, int]] = set()
    players_by_game_key: dict[tuple[str, int], list[NormalizedGamePlayerRecord]] = defaultdict(list)

    for game in batch.games:
        _validate_normalized_game_record(
            game,
            expected_team=batch.scope.team,
            expected_season=batch.scope.season,
        )
        game_key = (game.game_id, game.team.team_id)
        if game_key in game_keys:
            raise ValueError(f"Duplicate canonical game row for {game_key!r}")
        game_keys.add(game_key)

    player_keys: set[tuple[str, int, int]] = set()
    for player in batch.game_players:
        _validate_normalized_game_player_record(player, expected_team=batch.scope.team)
        player_key = (player.game_id, player.team.team_id, player.player.player_id)
        if player_key in player_keys:
            raise ValueError(f"Duplicate canonical player row for {player_key!r}")
        player_keys.add(player_key)
        players_by_game_key[(player.game_id, player.team.team_id)].append(player)

    if set(players_by_game_key) != game_keys:
        missing_players = sorted(game_keys - set(players_by_game_key))
        extra_players = sorted(set(players_by_game_key) - game_keys)
        raise ValueError(
            "Canonical game/player keys do not match: "
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


def _validate_normalized_game_record(
    game: NormalizedGameRecord,
    *,
    expected_team: Team,
    expected_season: Season,
) -> None:
    if not game.game_id.strip():
        raise ValueError("Canonical game_id must not be empty")
    if not Season.are_same(game.season, expected_season):
        raise ValueError(
            f"Canonical game {game.game_id!r} season {game.season!r}; expected {expected_season!r}"
        )
    if not Team.are_same(game.team, expected_team):
        raise ValueError(
            f"Canonical game {game.game_id!r} is not the same as {game.team!r}; "
            f"expected {expected_team!r}"
        )
    if game.opponent_team.team_id <= 0:
        raise ValueError(f"Canonical game {game.game_id!r} must have a positive opponent_team_id")
    if game.opponent_team.team_id == expected_team.team_id:
        raise ValueError(
            f"Canonical game {game.game_id!r} must not use the same team_id and opponent_team_id"
        )
    game.team.validate()
    game.opponent_team.validate()
    if not _GAME_DATE_PATTERN.fullmatch(game.game_date):
        raise ValueError(f"Canonical game {game.game_id!r} has invalid date {game.game_date!r}")
    parsed_date = date.fromisoformat(game.game_date)
    if parsed_date.year not in {expected_season.start_year, expected_season.start_year + 1}:
        raise ValueError(
            f"Canonical game {game.game_id!r} date {game.game_date!r} "
            f"falls outside season {expected_season!r}"
        )
    if not math.isfinite(game.margin):
        raise ValueError(f"Canonical game {game.game_id!r} has non-finite margin")
    if not game.source.strip():
        raise ValueError(f"Canonical game {game.game_id!r} must have a non-empty source")


def _validate_normalized_game_player_record(
    player: NormalizedGamePlayerRecord,
    *,
    expected_team: Team,
) -> None:
    player_ref = (
        "game "
        f"{player.game_id!r} player_id={player.player.player_id!r} "
        f"player_name={player.player.player_name!r}"
    )
    if not player.game_id.strip():
        raise ValueError("Canonical player game_id must not be empty")
    if player.team.team_id != expected_team.team_id:
        raise ValueError(
            f"Canonical player row for game {player.game_id!r} has team_id "
            f"{player.team.team_id!r}; expected {expected_team.team_id!r}"
        )
    if player.player.player_id <= 0:
        raise ValueError(f"Canonical player row for {player_ref} has invalid player_id")
    if not player.player.player_name.strip():
        raise ValueError(f"Canonical player row for {player_ref} must have a player name")
    if player.minutes is None:
        raise ValueError(f"Canonical player row for {player_ref} must have minutes")

    minutes = player.minutes
    if not math.isfinite(minutes) or minutes < 0.0:
        raise ValueError(f"Canonical player row for {player_ref} has invalid minutes {minutes!r}")


__all__ = [
    "validate_normalized_team_season_batch",
]
