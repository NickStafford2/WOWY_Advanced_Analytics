from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import date

from rawr_analytics.nba.normalize.models import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
)
from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.nba.seasons import canonicalize_season_year_string
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_TEAM_ABBREVIATION_PATTERN = re.compile(r"^[A-Z0-9]{2,4}$")
_GAME_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_normalized_team_season_batch(batch: NormalizedTeamSeasonBatch) -> None:
    game_keys: set[tuple[str, int]] = set()
    players_by_game_key: dict[tuple[str, int], list[NormalizedGamePlayerRecord]] = defaultdict(list)

    for game in batch.games:
        _validate_canonical_game(
            game,
            expected_team_id=batch.team.team_id,
            expected_season=batch.season,
            expected_season_type=batch.season_type,
        )
        game_key = (game.game_id, game.team_id)
        if game_key in game_keys:
            raise ValueError(f"Duplicate canonical game row for {game_key!r}")
        game_keys.add(game_key)

    player_keys: set[tuple[str, int, int]] = set()
    for player in batch.game_players:
        _validate_canonical_game_player(
            player,
            expected_team_id=batch.team_id,
        )
        player_key = (player.game_id, player.team_id, player.player_id)
        if player_key in player_keys:
            raise ValueError(f"Duplicate canonical player row for {player_key!r}")
        player_keys.add(player_key)
        players_by_game_key[(player.game_id, player.team_id)].append(player)

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
        total_minutes = sum(player.minutes or 0.0 for player in appeared_players)
        if total_minutes < 220.0 or total_minutes > 450.0:
            raise ValueError(
                f"Implausible total appeared minutes for {game_key!r}: {total_minutes}"
            )


def validate_normalized_cache_batch(
    *,
    team: Team,
    season: Season,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> None:
    batch = NormalizedTeamSeasonBatch(
        team=team,
        season=season,
        games=games,
        game_players=game_players,
    )
    validate_normalized_team_season_batch(batch)


def _validate_canonical_game(
    game: NormalizedGameRecord,
    *,
    expected_team: Team,
    expected_season: Season,
) -> None:
    if not game.game_id.strip():
        raise ValueError("Canonical game_id must not be empty")
    if canonicalize_season_year_string(game.season) != expected_season:
        raise ValueError(
            f"Canonical game {game.game_id!r} has season {game.season!r}; "
            f"expected {expected_season!r}"
        )
    if canonicalize_season_type(game.season_type) != expected_season.season_type:
        raise ValueError(
            f"Canonical game {game.game_id!r} has season type {game.season_type!r}; "
            f"expected {expected_season.season_type!r}"
        )
    if game.team.team_id != expected_team.team_id:
        raise ValueError(
            f"Canonical game {game.game_id!r} has team_id {game.team.team_id!r}; "
            f"expected {expected_team.team_id!r}"
        )
    if game.opponent_team.team_id is None or game.opponent_team.team_id <= 0:
        raise ValueError(f"Canonical game {game.game_id!r} must have a positive opponent_team_id")
    if game.opponent_team.team_id == expected_team.team_id:
        raise ValueError(
            f"Canonical game {game.game_id!r} must not use the same team_id and opponent_team_id"
        )
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


def _validate_canonical_game_player(
    player: NormalizedGamePlayerRecord,
    *,
    expected_team_id: int,
) -> None:
    player_ref = (
        f"game {player.game_id!r} player_id={player.player_id!r} player_name={player.player_name!r}"
    )
    if not player.game_id.strip():
        raise ValueError("Canonical player game_id must not be empty")
    if player.team_id != expected_team_id:
        raise ValueError(
            f"Canonical player row for game {player.game_id!r} has team_id {player.team_id!r}; "
            f"expected {expected_team_id!r}"
        )
    if player.player_id <= 0:
        raise ValueError(f"Canonical player row for {player_ref} has invalid player_id")
    if not player.player_name.strip():
        raise ValueError(f"Canonical player row for {player_ref} must have a player name")

    minutes = player.minutes
    if minutes is not None:
        if not math.isfinite(minutes) or minutes < 0.0:
            raise ValueError(
                f"Canonical player row for {player_ref} has invalid minutes {minutes!r}"
            )
        if minutes > 80.0:
            raise ValueError(
                f"Canonical player row for {player_ref} has implausible minutes {minutes!r}"
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


__all__ = [
    "validate_normalized_cache_batch",
    "validate_normalized_team_season_batch",
    "_canonical_team_abbreviation",
]
