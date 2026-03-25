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
from rawr_analytics.nba.seasons import canonicalize_season_string
from rawr_analytics.nba.team_identity import (
    resolve_team_identity_from_id_and_date,
    resolve_team_identity_from_id_and_season,
)

_TEAM_ABBREVIATION_PATTERN = re.compile(r"^[A-Z0-9]{2,4}$")
_GAME_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_normalized_team_season_batch(batch: NormalizedTeamSeasonBatch) -> None:
    expected_batch_identity = resolve_team_identity_from_id_and_season(batch.team_id, batch.season)
    if batch.team != expected_batch_identity.abbreviation:
        raise ValueError(
            f"Canonical batch team {batch.team!r} does not match team_id {batch.team_id!r} "
            f"for season {batch.season!r}; expected {expected_batch_identity.abbreviation!r}"
        )

    game_keys: set[tuple[str, int]] = set()
    players_by_game_key: dict[tuple[str, int], list[NormalizedGamePlayerRecord]] = defaultdict(list)

    for game in batch.games:
        _validate_canonical_game(
            game,
            expected_team=batch.team,
            expected_team_id=batch.team_id,
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
            expected_team=batch.team,
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
    team: str,
    team_id: int,
    season: str,
    season_type: str,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> None:
    batch = NormalizedTeamSeasonBatch(
        team=_canonical_team_abbreviation(team),
        team_id=team_id,
        season=canonicalize_season_string(season),
        season_type=canonicalize_season_type(season_type),
        games=games,
        game_players=game_players,
    )
    validate_normalized_team_season_batch(batch)


def _validate_canonical_game(
    game: NormalizedGameRecord,
    *,
    expected_team: str,
    expected_team_id: int,
    expected_season: str,
    expected_season_type: str,
) -> None:
    if not game.game_id.strip():
        raise ValueError("Canonical game_id must not be empty")
    if canonicalize_season_string(game.season) != expected_season:
        raise ValueError(
            f"Canonical game {game.game_id!r} has season {game.season!r}; "
            f"expected {expected_season!r}"
        )
    if canonicalize_season_type(game.season_type) != expected_season_type:
        raise ValueError(
            f"Canonical game {game.game_id!r} has season type {game.season_type!r}; "
            f"expected {expected_season_type!r}"
        )
    if _canonical_team_abbreviation(game.team) != _canonical_team_abbreviation(expected_team):
        raise ValueError(
            f"Canonical game {game.game_id!r} has team {game.team!r}; expected {expected_team!r}"
        )
    if game.team_id != expected_team_id:
        raise ValueError(
            f"Canonical game {game.game_id!r} has team_id {game.team_id!r}; "
            f"expected {expected_team_id!r}"
        )
    if game.opponent_team_id is None or game.opponent_team_id <= 0:
        raise ValueError(f"Canonical game {game.game_id!r} must have a positive opponent_team_id")
    if game.opponent_team_id == expected_team_id:
        raise ValueError(
            f"Canonical game {game.game_id!r} must not use the same team_id and opponent_team_id"
        )
    if _canonical_team_abbreviation(game.opponent) == _canonical_team_abbreviation(expected_team):
        raise ValueError(f"Canonical game {game.game_id!r} must not use the same team and opponent")
    expected_team_identity = resolve_team_identity_from_id_and_date(
        expected_team_id,
        game.game_date,
    )
    if game.team != expected_team_identity.abbreviation:
        raise ValueError(
            f"Canonical game {game.game_id!r} has team {game.team!r}; "
            f"expected historical abbreviation {expected_team_identity.abbreviation!r}"
        )
    expected_opponent_identity = resolve_team_identity_from_id_and_date(
        game.opponent_team_id,
        game.game_date,
    )
    if game.opponent != expected_opponent_identity.abbreviation:
        raise ValueError(
            f"Canonical game {game.game_id!r} opponent {game.opponent!r} "
            f"does not match opponent_team_id {game.opponent_team_id!r} "
            f"for {game.game_date!r}"
        )
    if not _GAME_DATE_PATTERN.fullmatch(game.game_date):
        raise ValueError(f"Canonical game {game.game_id!r} has invalid date {game.game_date!r}")
    parsed_date = date.fromisoformat(game.game_date)
    start_year = int(expected_season[:4])
    if parsed_date.year not in {start_year, start_year + 1}:
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
    expected_team: str,
    expected_team_id: int,
) -> None:
    player_ref = (
        f"game {player.game_id!r} player_id={player.player_id!r} player_name={player.player_name!r}"
    )
    if not player.game_id.strip():
        raise ValueError("Canonical player game_id must not be empty")
    if _canonical_team_abbreviation(player.team) != _canonical_team_abbreviation(expected_team):
        raise ValueError(
            f"Canonical player row for game {player.game_id!r} has team {player.team!r}; "
            f"expected {expected_team!r}"
        )
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
]
