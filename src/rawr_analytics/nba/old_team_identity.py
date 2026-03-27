from __future__ import annotations

import re
from dataclasses import dataclass
from typing import NoReturn

from rawr_analytics.nba import team_history as _team_history
from rawr_analytics.nba.team_history import (
    TeamHistoryEntry,
    canonical_team_lookup_abbreviation,
    list_team_history_entries_for_abbreviation,
    normalize_team_abbreviation,
    resolve_team_history_entry,
    resolve_team_history_entry_for_date,
    resolve_team_history_entry_from_id,
    resolve_team_history_entry_from_id_for_date,
    resolve_team_id_for_lookup,
)


@dataclass(frozen=True)
class TeamIdentity:
    team_id: int
    abbreviation: str
    franchise_id: str
    lookup_abbreviation: str


list_expected_team_abbreviations_for_season = (
    _team_history.list_expected_team_abbreviations_for_season
)
team_is_active_for_season = _team_history.team_is_active_for_season
_INTEGER_TEXT_RE = re.compile(r"^[+-]?\d+$")


def _identity_from_entry(entry: TeamHistoryEntry) -> TeamIdentity:
    return TeamIdentity(
        team_id=entry.team_id,
        abbreviation=entry.abbreviation,
        franchise_id=entry.franchise_id,
        lookup_abbreviation=entry.lookup_abbreviation or entry.abbreviation,
    )


def resolve_team_identity(
    team_abbreviation: str,
    *,
    season: str | None = None,
    game_date: str | None = None,
) -> TeamIdentity:
    normalized = normalize_team_abbreviation(team_abbreviation)
    if game_date is not None:
        try:
            return _identity_from_entry(
                resolve_team_history_entry_for_date(normalized, game_date=game_date)
            )
        except ValueError:
            return resolve_team_identity_from_id_and_date(
                resolve_team_id_for_lookup(normalized),
                game_date,
            )
    if season is not None:
        try:
            return _identity_from_entry(resolve_team_history_entry(normalized, season=season))
        except ValueError:
            return resolve_team_identity_from_id_and_season(
                resolve_team_id_for_lookup(normalized),
                season,
            )
    lookup_abbreviation = canonical_team_lookup_abbreviation(normalized)
    return TeamIdentity(
        team_id=resolve_team_id_for_lookup(normalized),
        abbreviation=lookup_abbreviation,
        franchise_id=lookup_abbreviation.lower(),
        lookup_abbreviation=lookup_abbreviation,
    )


def resolve_team_id(
    team_abbreviation: str,
    *,
    season: str | None = None,
    game_date: str | None = None,
) -> int:
    return resolve_team_identity(
        team_abbreviation,
        season=season,
        game_date=game_date,
    ).team_id


def resolve_team_identity_from_id_and_season(team_id: int, season: str) -> TeamIdentity:
    return _identity_from_entry(resolve_team_history_entry_from_id(team_id, season=season))


def resolve_team_identity_from_id_and_date(team_id: int, game_date: str) -> TeamIdentity:
    return _identity_from_entry(
        resolve_team_history_entry_from_id_for_date(team_id, game_date=game_date)
    )


def resolve_team_abbreviation_for_id(
    team_id: int,
    *,
    season: str | None = None,
    game_date: str | None = None,
) -> str:
    if game_date is not None:
        return resolve_team_identity_from_id_and_date(team_id, game_date).abbreviation
    if season is not None:
        return resolve_team_identity_from_id_and_season(team_id, season).abbreviation
    raise ValueError("resolve_team_abbreviation_for_id requires a season or game date")


def resolve_source_team_identity(
    *,
    team_id: object | None,
    team_abbreviation: str | None,
    fallback_team_id: int | None = None,
    fallback_abbreviation: str | None = None,
    season: str | None = None,
    game_date: str | None = None,
    error_context: str | None = None,
) -> TeamIdentity:
    normalized_abbreviation = (team_abbreviation or "").strip().upper()
    normalized_fallback_abbreviation = (fallback_abbreviation or "").strip().upper()

    if team_id is not None:
        parsed_team_id = _parse_source_team_id(team_id, error_context=error_context)
        if parsed_team_id <= 0:
            _raise_team_identity_error(f"Invalid TEAM_ID value: {team_id!r}", error_context)
    else:
        parsed_team_id = None

    if fallback_team_id is not None and fallback_team_id <= 0:
        _raise_team_identity_error(
            f"Invalid fallback_team_id value: {fallback_team_id!r}",
            error_context,
        )

    if (
        parsed_team_id is not None
        and fallback_team_id is not None
        and parsed_team_id != fallback_team_id
    ):
        _raise_team_identity_error(
            "Conflicting source team identity values: "
            f"TEAM_ID={parsed_team_id!r} fallback_team_id={fallback_team_id!r}",
            error_context,
        )

    resolved_team_id = parsed_team_id or fallback_team_id
    resolved_abbreviation = normalized_abbreviation or normalized_fallback_abbreviation

    if resolved_team_id is None and not resolved_abbreviation:
        _raise_team_identity_error("Missing team identity values", error_context)

    if resolved_team_id is None:
        return resolve_team_identity(
            resolved_abbreviation,
            season=season,
            game_date=game_date,
        )

    if game_date is not None:
        identity = resolve_team_identity_from_id_and_date(resolved_team_id, game_date)
    elif season is not None:
        identity = resolve_team_identity_from_id_and_season(resolved_team_id, season)
    else:
        identity = None

    if identity is not None:
        for abbreviation_label, abbreviation_value in (
            ("TEAM_ABBREVIATION", normalized_abbreviation),
            ("fallback_abbreviation", normalized_fallback_abbreviation),
        ):
            if abbreviation_value and abbreviation_value != identity.abbreviation:
                _raise_team_identity_error(
                    "Conflicting source team identity values: "
                    f"{abbreviation_label}={abbreviation_value!r} "
                    f"expected={identity.abbreviation!r} "
                    f"for TEAM_ID={resolved_team_id!r}",
                    error_context,
                )
        return identity

    if resolved_abbreviation:
        normalize_team_abbreviation(resolved_abbreviation)
        historical_team_ids = {
            entry.team_id
            for entry in list_team_history_entries_for_abbreviation(resolved_abbreviation)
        }
        if resolved_team_id not in historical_team_ids:
            _raise_team_identity_error(
                "Conflicting source team identity values: "
                f"TEAM_ID={resolved_team_id!r} TEAM_ABBREVIATION={resolved_abbreviation!r}",
                error_context,
            )
        return TeamIdentity(
            team_id=resolved_team_id,
            abbreviation=resolved_abbreviation,
            franchise_id=canonical_team_lookup_abbreviation(resolved_abbreviation).lower(),
            lookup_abbreviation=canonical_team_lookup_abbreviation(resolved_abbreviation),
        )

    lookup_abbreviation = canonical_team_lookup_abbreviation(normalized_fallback_abbreviation)
    return TeamIdentity(
        team_id=resolved_team_id,
        abbreviation=normalized_fallback_abbreviation,
        franchise_id=lookup_abbreviation.lower(),
        lookup_abbreviation=lookup_abbreviation,
    )


def _parse_source_team_id(team_id: object, *, error_context: str | None) -> int:
    if isinstance(team_id, bool):
        _raise_team_identity_error(f"Invalid TEAM_ID value: {team_id!r}", error_context)
    if isinstance(team_id, int):
        return team_id
    if isinstance(team_id, float):
        if not team_id.is_integer():
            _raise_team_identity_error(f"Invalid TEAM_ID value: {team_id!r}", error_context)
        return int(team_id)
    if isinstance(team_id, str) and _INTEGER_TEXT_RE.fullmatch(team_id.strip()):
        return int(team_id.strip())
    _raise_team_identity_error(f"Invalid TEAM_ID value: {team_id!r}", error_context)
    assert False, "unreachable after _raise_team_identity_error"


def _raise_team_identity_error(message: str, error_context: str | None) -> NoReturn:
    if error_context:
        raise ValueError(f"{message}; {error_context}")
    raise ValueError(message)
