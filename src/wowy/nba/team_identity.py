from __future__ import annotations

from dataclasses import dataclass

from nba_api.stats.static import teams as nba_teams


@dataclass(frozen=True)
class TeamIdentity:
    team_id: int
    abbreviation: str


_CURRENT_TEAMS_BY_ABBREVIATION = {
    team["abbreviation"]: TeamIdentity(
        team_id=int(team["id"]),
        abbreviation=str(team["abbreviation"]),
    )
    for team in nba_teams.get_teams()
}
_CURRENT_TEAMS_BY_ID = {
    identity.team_id: identity
    for identity in _CURRENT_TEAMS_BY_ABBREVIATION.values()
}

_HISTORICAL_ALIAS_TO_CURRENT = {
    "CHH": "CHA",
    "NJN": "BKN",
    "NOH": "NOP",
    "NOK": "NOP",
    "SEA": "OKC",
    "VAN": "MEM",
    "WSB": "WAS",
}


def resolve_team_identity(team_abbreviation: str) -> TeamIdentity:
    normalized = team_abbreviation.strip().upper()
    lookup_abbreviation = _HISTORICAL_ALIAS_TO_CURRENT.get(normalized, normalized)
    identity = _CURRENT_TEAMS_BY_ABBREVIATION.get(lookup_abbreviation)
    if identity is None:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")
    return TeamIdentity(team_id=identity.team_id, abbreviation=normalized)


def resolve_team_id(team_abbreviation: str) -> int:
    return resolve_team_identity(team_abbreviation).team_id


def resolve_team_identity_from_id(team_id: int) -> TeamIdentity:
    identity = _CURRENT_TEAMS_BY_ID.get(team_id)
    if identity is None:
        raise ValueError(f"Unknown NBA team id: {team_id!r}")
    return identity


def canonical_team_lookup_abbreviation(team_abbreviation: str) -> str:
    normalized = team_abbreviation.strip().upper()
    return _HISTORICAL_ALIAS_TO_CURRENT.get(normalized, normalized)


def resolve_source_team_identity(
    *,
    team_id: object | None,
    team_abbreviation: str | None,
    fallback_team_id: int | None = None,
    fallback_abbreviation: str | None = None,
) -> TeamIdentity:
    normalized_abbreviation = (
        (team_abbreviation or fallback_abbreviation or "").strip().upper()
    )
    if team_id is not None:
        try:
            parsed_team_id = int(team_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid TEAM_ID value: {team_id!r}") from exc
        if parsed_team_id <= 0:
            raise ValueError(f"Invalid TEAM_ID value: {team_id!r}")
        if normalized_abbreviation:
            abbreviation_identity = resolve_team_identity(normalized_abbreviation)
            if abbreviation_identity.team_id != parsed_team_id:
                raise ValueError(
                    "Conflicting source team identity values: "
                    f"TEAM_ID={parsed_team_id!r} TEAM_ABBREVIATION={normalized_abbreviation!r}"
                )
            return TeamIdentity(team_id=parsed_team_id, abbreviation=normalized_abbreviation)
        if fallback_abbreviation:
            fallback_identity = resolve_team_identity(fallback_abbreviation)
            if fallback_identity.team_id != parsed_team_id:
                raise ValueError(
                    "Conflicting source team identity values: "
                    f"TEAM_ID={parsed_team_id!r} fallback_abbreviation={fallback_abbreviation!r}"
                )
            return TeamIdentity(
                team_id=parsed_team_id,
                abbreviation=fallback_abbreviation.strip().upper(),
            )
        return TeamIdentity(
            team_id=parsed_team_id,
            abbreviation=resolve_team_identity_from_id(parsed_team_id).abbreviation,
        )

    if fallback_team_id is not None:
        if normalized_abbreviation:
            abbreviation_identity = resolve_team_identity(normalized_abbreviation)
            if abbreviation_identity.team_id != fallback_team_id:
                raise ValueError(
                    "Conflicting source team identity values: "
                    f"fallback_team_id={fallback_team_id!r} "
                    f"TEAM_ABBREVIATION={normalized_abbreviation!r}"
                )
            return TeamIdentity(team_id=fallback_team_id, abbreviation=normalized_abbreviation)
        if fallback_abbreviation:
            fallback_identity = resolve_team_identity(fallback_abbreviation)
            if fallback_identity.team_id != fallback_team_id:
                raise ValueError(
                    "Conflicting source team identity values: "
                    f"fallback_team_id={fallback_team_id!r} "
                    f"fallback_abbreviation={fallback_abbreviation!r}"
                )
            return TeamIdentity(
                team_id=fallback_team_id,
                abbreviation=fallback_abbreviation.strip().upper(),
            )

    if not normalized_abbreviation:
        raise ValueError("Missing team identity values")
    identity = resolve_team_identity(normalized_abbreviation)
    return TeamIdentity(team_id=identity.team_id, abbreviation=normalized_abbreviation)
