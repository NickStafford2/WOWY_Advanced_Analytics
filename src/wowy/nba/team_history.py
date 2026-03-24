from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from wowy.nba.seasons import canonicalize_season_string


@dataclass(frozen=True)
class TeamHistoryEntry:
    """
    One time-bounded public identity of a franchise.

    This is not just a "brand name". It is the specific team identity used in a
    real historical window:

    - abbreviation used in that era
    - NBA/source team_id used for rows in that era
    - season range where that identity was active

    A franchise usually has multiple TeamHistoryEntry values because the same
    continuity can appear under different public identities over time.
    """

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


TEAM_HISTORY: tuple[TeamHistoryEntry, ...] = (
    TeamHistoryEntry("PHW", 1610612744, "warriors", 1946, 1961, "GSW"),
    TeamHistoryEntry("SFW", 1610612744, "warriors", 1962, 1970, "GSW"),
    TeamHistoryEntry("GSW", 1610612744, "warriors", 1971),
    TeamHistoryEntry("BOS", 1610612738, "celtics", 1946),
    TeamHistoryEntry("NYK", 1610612752, "knicks", 1946),
    TeamHistoryEntry("MNL", 1610612747, "lakers", 1948, 1959, "LAL"),
    TeamHistoryEntry("LAL", 1610612747, "lakers", 1960),
    TeamHistoryEntry("ROC", 1610612758, "kings", 1948, 1956, "SAC"),
    TeamHistoryEntry("CIN", 1610612758, "kings", 1957, 1971, "SAC"),
    TeamHistoryEntry("KCO", 1610612758, "kings", 1972, 1974, "SAC"),
    TeamHistoryEntry("KCK", 1610612758, "kings", 1975, 1984, "SAC"),
    TeamHistoryEntry("SAC", 1610612758, "kings", 1985),
    TeamHistoryEntry("FTW", 1610612765, "pistons", 1948, 1956, "DET"),
    TeamHistoryEntry("DET", 1610612765, "pistons", 1957),
    TeamHistoryEntry("TRI", 1610612737, "hawks", 1949, 1950, "ATL"),
    TeamHistoryEntry("MLH", 1610612737, "hawks", 1951, 1954, "ATL"),
    TeamHistoryEntry("STL", 1610612737, "hawks", 1955, 1967, "ATL"),
    TeamHistoryEntry("ATL", 1610612737, "hawks", 1968),
    TeamHistoryEntry("SYR", 1610612755, "76ers", 1949, 1962, "PHI"),
    TeamHistoryEntry("PHI", 1610612755, "76ers", 1963),
    TeamHistoryEntry("CHP", 1610612764, "wizards", 1961, 1961, "WAS"),
    TeamHistoryEntry("CHZ", 1610612764, "wizards", 1962, 1962, "WAS"),
    TeamHistoryEntry("BAL", 1610612764, "wizards", 1963, 1972, "WAS"),
    TeamHistoryEntry("CAP", 1610612764, "wizards", 1973, 1973, "WAS"),
    TeamHistoryEntry("WSB", 1610612764, "wizards", 1974, 1996, "WAS"),
    TeamHistoryEntry("WAS", 1610612764, "wizards", 1997),
    TeamHistoryEntry("CHI", 1610612741, "bulls", 1966),
    TeamHistoryEntry("SEA", 1610612760, "thunder_supersonics", 1967, 2007, "OKC"),
    TeamHistoryEntry("OKC", 1610612760, "thunder_supersonics", 2008),
    TeamHistoryEntry("SDR", 1610612745, "rockets", 1967, 1970, "HOU"),
    TeamHistoryEntry("HOU", 1610612745, "rockets", 1971),
    TeamHistoryEntry("MIL", 1610612749, "bucks", 1968),
    TeamHistoryEntry("PHX", 1610612756, "suns", 1968),
    TeamHistoryEntry("BUF", 1610612746, "clippers", 1970, 1977, "LAC"),
    TeamHistoryEntry("SDC", 1610612746, "clippers", 1978, 1983, "LAC"),
    TeamHistoryEntry("LAC", 1610612746, "clippers", 1984),
    TeamHistoryEntry("CLE", 1610612739, "cavaliers", 1970),
    TeamHistoryEntry("POR", 1610612757, "trail_blazers", 1970),
    TeamHistoryEntry("NOJ", 1610612762, "jazz", 1974, 1978, "UTA"),
    TeamHistoryEntry("UTA", 1610612762, "jazz", 1979),
    TeamHistoryEntry("DEN", 1610612743, "nuggets", 1976),
    TeamHistoryEntry("IND", 1610612754, "pacers", 1976),
    TeamHistoryEntry("NJN", 1610612751, "nets", 1976, 2011, "BKN"),
    TeamHistoryEntry("BKN", 1610612751, "nets", 2012),
    TeamHistoryEntry("SAS", 1610612759, "spurs", 1976),
    TeamHistoryEntry("DAL", 1610612742, "mavericks", 1980),
    TeamHistoryEntry("CHH", 1610612766, "hornets_original", 1988, 2001, "NOP"),
    TeamHistoryEntry("MIA", 1610612748, "heat", 1988),
    TeamHistoryEntry("MIN", 1610612750, "timberwolves", 1989),
    TeamHistoryEntry("ORL", 1610612753, "magic", 1989),
    TeamHistoryEntry("NOH", 1610612740, "hornets_original", 2002, 2004, "NOP"),
    TeamHistoryEntry("NOK", 1610612740, "hornets_original", 2005, 2006, "NOP"),
    TeamHistoryEntry("NOH", 1610612740, "hornets_original", 2007, 2012, "NOP"),
    TeamHistoryEntry("NOP", 1610612740, "hornets_original", 2013),
    TeamHistoryEntry("CHA", 1610612766, "charlotte_expansion", 2004),
    TeamHistoryEntry("TOR", 1610612761, "raptors", 1995),
    TeamHistoryEntry("VAN", 1610612763, "grizzlies", 1995, 2000, "MEM"),
    TeamHistoryEntry("MEM", 1610612763, "grizzlies", 2001),
)

_TEAM_HISTORY_BY_ABBREVIATION: dict[str, list[TeamHistoryEntry]] = {}
_TEAM_HISTORY_BY_ID: dict[int, list[TeamHistoryEntry]] = {}
_LOOKUP_ABBREVIATION_BY_CODE: dict[str, str] = {}
_TEAM_ID_BY_LOOKUP_ABBREVIATION: dict[str, int] = {}

for entry in TEAM_HISTORY:
    _TEAM_HISTORY_BY_ABBREVIATION.setdefault(entry.abbreviation, []).append(entry)
    _TEAM_HISTORY_BY_ID.setdefault(entry.team_id, []).append(entry)
    lookup_abbreviation = entry.lookup_abbreviation or entry.abbreviation
    _LOOKUP_ABBREVIATION_BY_CODE[entry.abbreviation] = lookup_abbreviation

for entries in _TEAM_HISTORY_BY_ABBREVIATION.values():
    entries.sort(key=lambda entry: (entry.season_start, entry.season_end or 9999))
for entries in _TEAM_HISTORY_BY_ID.values():
    entries.sort(key=lambda entry: (entry.season_start, entry.season_end or 9999))

for entries in _TEAM_HISTORY_BY_ID.values():
    latest_entry = max(
        entries, key=lambda entry: (entry.season_end or 9999, entry.season_start)
    )
    lookup_abbreviation = latest_entry.lookup_abbreviation or latest_entry.abbreviation
    _TEAM_ID_BY_LOOKUP_ABBREVIATION.setdefault(
        lookup_abbreviation, latest_entry.team_id
    )


def normalize_team_abbreviation(team_abbreviation: str) -> str:
    normalized = team_abbreviation.strip().upper()
    if not normalized:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")
    if (
        normalized not in _LOOKUP_ABBREVIATION_BY_CODE
        and normalized not in _TEAM_ID_BY_LOOKUP_ABBREVIATION
    ):
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")
    return normalized


def canonical_team_lookup_abbreviation(team_abbreviation: str) -> str:
    normalized = normalize_team_abbreviation(team_abbreviation)
    return _LOOKUP_ABBREVIATION_BY_CODE.get(normalized, normalized)


def season_start_year(season: str) -> int:
    return int(canonicalize_season_string(season)[:4])


def season_start_year_from_game_date(game_date: str) -> int:
    parsed_date = date.fromisoformat(game_date)
    return parsed_date.year if parsed_date.month >= 7 else parsed_date.year - 1


def resolve_team_history_entry(
    team_abbreviation: str,
    *,
    season: str,
) -> TeamHistoryEntry:
    return resolve_team_history_entry_for_season_start_year(
        team_abbreviation,
        season_start=season_start_year(season),
    )


def resolve_team_history_entry_for_date(
    team_abbreviation: str,
    *,
    game_date: str,
) -> TeamHistoryEntry:
    return resolve_team_history_entry_for_season_start_year(
        team_abbreviation,
        season_start=season_start_year_from_game_date(game_date),
    )


def resolve_team_history_entry_for_season_start_year(
    team_abbreviation: str,
    *,
    season_start: int,
) -> TeamHistoryEntry:
    normalized = normalize_team_abbreviation(team_abbreviation)
    for entry in _TEAM_HISTORY_BY_ABBREVIATION.get(normalized, []):
        if entry.includes_season(season_start):
            return entry
    raise ValueError(
        f"Team {normalized!r} was not active in season {canonicalize_season_string(str(season_start))!r}"
    )


def resolve_team_history_entry_from_id(
    team_id: int,
    *,
    season: str,
) -> TeamHistoryEntry:
    return resolve_team_history_entry_from_id_for_season_start_year(
        team_id,
        season_start=season_start_year(season),
    )


def resolve_team_history_entry_from_id_for_date(
    team_id: int,
    *,
    game_date: str,
) -> TeamHistoryEntry:
    return resolve_team_history_entry_from_id_for_season_start_year(
        team_id,
        season_start=season_start_year_from_game_date(game_date),
    )


def resolve_team_history_entry_from_id_for_season_start_year(
    team_id: int,
    *,
    season_start: int,
) -> TeamHistoryEntry:
    for entry in _TEAM_HISTORY_BY_ID.get(team_id, []):
        if entry.includes_season(season_start):
            return entry
    raise ValueError(
        f"Team id {team_id!r} was not active in season {canonicalize_season_string(str(season_start))!r}"
    )


def list_expected_team_abbreviations_for_season(season: str) -> list[str]:
    start_year = season_start_year(season)
    return sorted(
        entry.abbreviation
        for entry in TEAM_HISTORY
        if entry.includes_season(start_year)
    )


def team_is_active_for_season(team_abbreviation: str, season: str) -> bool:
    try:
        resolve_team_history_entry(team_abbreviation, season=season)
    except ValueError:
        return False
    return True


def list_team_history_entries_for_abbreviation(
    team_abbreviation: str,
) -> list[TeamHistoryEntry]:
    normalized = normalize_team_abbreviation(team_abbreviation)
    return list(_TEAM_HISTORY_BY_ABBREVIATION.get(normalized, []))


def resolve_team_id_for_lookup(team_abbreviation: str) -> int:
    lookup_abbreviation = canonical_team_lookup_abbreviation(team_abbreviation)
    team_id = _TEAM_ID_BY_LOOKUP_ABBREVIATION.get(lookup_abbreviation)
    if team_id is None:
        raise ValueError(f"Unknown NBA team abbreviation: {team_abbreviation!r}")
    return team_id


def official_continuity_label_for_team_id(team_id: int) -> str:
    entries = _TEAM_HISTORY_BY_ID.get(team_id)
    if not entries:
        raise ValueError(f"Unknown NBA team id: {team_id!r}")
    latest_entry = max(
        entries,
        key=lambda entry: (entry.season_end or 9999, entry.season_start),
    )
    return latest_entry.lookup_abbreviation or latest_entry.abbreviation
