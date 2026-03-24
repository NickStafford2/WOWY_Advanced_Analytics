from __future__ import annotations

from dataclasses import dataclass

from nba_api.stats.static import teams as nba_teams

from wowy.nba.seasons import canonicalize_season_string


@dataclass(frozen=True)
class TeamIdentity:
    team_id: int
    abbreviation: str
    franchise_id: str | None = None


@dataclass(frozen=True)
class TeamHistoryEntry:
    abbreviation: str
    team_id: int
    franchise_id: str
    season_start: int
    season_end: int | None = None
    lookup_abbreviation: str | None = None

    def includes_season(self, season_start_year: int) -> bool:
        if season_start_year < self.season_start:
            return False
        if self.season_end is not None and season_start_year > self.season_end:
            return False
        return True


_CURRENT_TEAMS_BY_ABBREVIATION = {
    str(team["abbreviation"]).upper(): TeamIdentity(
        team_id=int(team["id"]),
        abbreviation=str(team["abbreviation"]).upper(),
        franchise_id=str(team["abbreviation"]).upper().lower(),
    )
    for team in nba_teams.get_teams()
}
_CURRENT_TEAMS_BY_ID = {
    identity.team_id: identity
    for identity in _CURRENT_TEAMS_BY_ABBREVIATION.values()
}
_CURRENT_TEAM_FOUNDING_YEAR = {
    str(team["abbreviation"]).upper(): int(team["year_founded"])
    for team in nba_teams.get_teams()
}

_EXPLICIT_TEAM_HISTORY = (
    TeamHistoryEntry(
        abbreviation="BKN",
        team_id=1610612751,
        franchise_id="nets",
        season_start=2012,
        lookup_abbreviation="BKN",
    ),
    TeamHistoryEntry(
        abbreviation="NJN",
        team_id=1610612751,
        franchise_id="nets",
        season_start=1976,
        season_end=2011,
        lookup_abbreviation="BKN",
    ),
    TeamHistoryEntry(
        abbreviation="CHA",
        team_id=1610612766,
        franchise_id="charlotte_expansion",
        season_start=2004,
        lookup_abbreviation="CHA",
    ),
    TeamHistoryEntry(
        abbreviation="CHH",
        team_id=1610612766,
        franchise_id="hornets_original",
        season_start=1988,
        season_end=2001,
        lookup_abbreviation="NOP",
    ),
    TeamHistoryEntry(
        abbreviation="MEM",
        team_id=1610612763,
        franchise_id="grizzlies",
        season_start=2001,
        lookup_abbreviation="MEM",
    ),
    TeamHistoryEntry(
        abbreviation="VAN",
        team_id=1610612763,
        franchise_id="grizzlies",
        season_start=1995,
        season_end=2000,
        lookup_abbreviation="MEM",
    ),
    TeamHistoryEntry(
        abbreviation="NOK",
        team_id=1610612740,
        franchise_id="hornets_original",
        season_start=2005,
        season_end=2006,
        lookup_abbreviation="NOP",
    ),
    TeamHistoryEntry(
        abbreviation="NOH",
        team_id=1610612740,
        franchise_id="hornets_original",
        season_start=2002,
        season_end=2012,
        lookup_abbreviation="NOP",
    ),
    TeamHistoryEntry(
        abbreviation="NOP",
        team_id=1610612740,
        franchise_id="hornets_original",
        season_start=2013,
        lookup_abbreviation="NOP",
    ),
    TeamHistoryEntry(
        abbreviation="OKC",
        team_id=1610612760,
        franchise_id="thunder_supersonics",
        season_start=2008,
        lookup_abbreviation="OKC",
    ),
    TeamHistoryEntry(
        abbreviation="SEA",
        team_id=1610612760,
        franchise_id="thunder_supersonics",
        season_start=1967,
        season_end=2007,
        lookup_abbreviation="OKC",
    ),
    TeamHistoryEntry(
        abbreviation="WAS",
        team_id=1610612764,
        franchise_id="wizards",
        season_start=1997,
        lookup_abbreviation="WAS",
    ),
    TeamHistoryEntry(
        abbreviation="WSB",
        team_id=1610612764,
        franchise_id="wizards",
        season_start=1961,
        season_end=1996,
        lookup_abbreviation="WAS",
    ),
)

_DEFAULT_EXPLICIT_CODES = {entry.abbreviation for entry in _EXPLICIT_TEAM_HISTORY}
_TEAM_HISTORY = list(_EXPLICIT_TEAM_HISTORY)
for abbreviation, identity in _CURRENT_TEAMS_BY_ABBREVIATION.items():
    if abbreviation in _DEFAULT_EXPLICIT_CODES:
        continue
    _TEAM_HISTORY.append(
        TeamHistoryEntry(
            abbreviation=abbreviation,
            team_id=identity.team_id,
            franchise_id=abbreviation.lower(),
            season_start=_CURRENT_TEAM_FOUNDING_YEAR[abbreviation],
            lookup_abbreviation=abbreviation,
        )
    )

_TEAM_HISTORY_BY_ABBREVIATION: dict[str, list[TeamHistoryEntry]] = {}
for entry in _TEAM_HISTORY:
    _TEAM_HISTORY_BY_ABBREVIATION.setdefault(entry.abbreviation, []).append(entry)
for entries in _TEAM_HISTORY_BY_ABBREVIATION.values():
    entries.sort(key=lambda entry: (entry.season_start, entry.season_end or 9999))

_TEAM_HISTORY_BY_ID: dict[int, list[TeamHistoryEntry]] = {}
for entry in _TEAM_HISTORY:
    _TEAM_HISTORY_BY_ID.setdefault(entry.team_id, []).append(entry)
for entries in _TEAM_HISTORY_BY_ID.values():
    entries.sort(key=lambda entry: (entry.season_start, entry.season_end or 9999))

_LOOKUP_ABBREVIATION_BY_CODE = {
    entry.abbreviation: entry.lookup_abbreviation or entry.abbreviation
    for entry in _TEAM_HISTORY
}


def normalize_team_abbreviation(team_abbreviation: str) -> str:
    normalized = team_abbreviation.strip().upper()
    if not normalized:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")
    if normalized not in _TEAM_HISTORY_BY_ABBREVIATION and normalized not in _CURRENT_TEAMS_BY_ABBREVIATION:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")
    return normalized


def season_start_year(season: str) -> int:
    return int(canonicalize_season_string(season)[:4])


def resolve_team_history_entry(
    team_abbreviation: str,
    *,
    season: str,
) -> TeamHistoryEntry:
    normalized = normalize_team_abbreviation(team_abbreviation)
    start_year = season_start_year(season)
    for entry in _TEAM_HISTORY_BY_ABBREVIATION.get(normalized, []):
        if entry.includes_season(start_year):
            return entry
    raise ValueError(
        f"Team {normalized!r} was not active in season {canonicalize_season_string(season)!r}"
    )


def resolve_team_history_entry_from_id(
    team_id: int,
    *,
    season: str,
) -> TeamHistoryEntry:
    start_year = season_start_year(season)
    for entry in _TEAM_HISTORY_BY_ID.get(team_id, []):
        if entry.includes_season(start_year):
            return entry
    raise ValueError(
        f"Team id {team_id!r} was not active in season {canonicalize_season_string(season)!r}"
    )


def list_expected_team_abbreviations_for_season(season: str) -> list[str]:
    start_year = season_start_year(season)
    return sorted(
        entry.abbreviation
        for entry in _TEAM_HISTORY
        if entry.includes_season(start_year)
    )


def team_is_active_for_season(team_abbreviation: str, season: str) -> bool:
    try:
        resolve_team_history_entry(team_abbreviation, season=season)
    except ValueError:
        return False
    return True


def resolve_team_identity(team_abbreviation: str, *, season: str | None = None) -> TeamIdentity:
    normalized = normalize_team_abbreviation(team_abbreviation)
    if season is not None:
        entry = resolve_team_history_entry(normalized, season=season)
        return TeamIdentity(
            team_id=entry.team_id,
            abbreviation=entry.abbreviation,
            franchise_id=entry.franchise_id,
        )

    entries = _TEAM_HISTORY_BY_ABBREVIATION.get(normalized)
    if entries:
        entry = entries[0]
        return TeamIdentity(
            team_id=entry.team_id,
            abbreviation=entry.abbreviation,
            franchise_id=entry.franchise_id,
        )
    identity = _CURRENT_TEAMS_BY_ABBREVIATION.get(normalized)
    if identity is None:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")
    return identity


def resolve_team_id(team_abbreviation: str, *, season: str | None = None) -> int:
    return resolve_team_identity(team_abbreviation, season=season).team_id


def resolve_team_identity_from_id(team_id: int) -> TeamIdentity:
    identity = _CURRENT_TEAMS_BY_ID.get(team_id)
    if identity is None:
        raise ValueError(f"Unknown NBA team id: {team_id!r}")
    return identity


def resolve_team_identity_from_id_and_season(team_id: int, season: str) -> TeamIdentity:
    entry = resolve_team_history_entry_from_id(team_id, season=season)
    return TeamIdentity(
        team_id=entry.team_id,
        abbreviation=entry.abbreviation,
        franchise_id=entry.franchise_id,
    )


def resolve_team_abbreviation_for_id(team_id: int, season: str) -> str:
    return resolve_team_identity_from_id_and_season(team_id, season).abbreviation


def canonical_team_lookup_abbreviation(team_abbreviation: str) -> str:
    normalized = normalize_team_abbreviation(team_abbreviation)
    return _LOOKUP_ABBREVIATION_BY_CODE.get(normalized, normalized)


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
            return TeamIdentity(
                team_id=parsed_team_id,
                abbreviation=normalized_abbreviation,
                franchise_id=abbreviation_identity.franchise_id,
            )
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
                franchise_id=fallback_identity.franchise_id,
            )
        resolved_identity = resolve_team_identity_from_id(parsed_team_id)
        return TeamIdentity(
            team_id=parsed_team_id,
            abbreviation=resolved_identity.abbreviation,
            franchise_id=resolved_identity.franchise_id,
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
            return TeamIdentity(
                team_id=fallback_team_id,
                abbreviation=normalized_abbreviation,
                franchise_id=abbreviation_identity.franchise_id,
            )
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
                franchise_id=fallback_identity.franchise_id,
            )

    if not normalized_abbreviation:
        raise ValueError("Missing team identity values")
    return resolve_team_identity(normalized_abbreviation)
