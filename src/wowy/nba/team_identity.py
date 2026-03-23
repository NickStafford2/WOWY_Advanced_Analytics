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


def canonical_team_lookup_abbreviation(team_abbreviation: str) -> str:
    normalized = team_abbreviation.strip().upper()
    return _HISTORICAL_ALIAS_TO_CURRENT.get(normalized, normalized)
