from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType

from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class TeamSeason:
    team_id: int
    start_year: int
    city: str
    nickname: str
    abbreviation: str

    @property
    def full_name(self) -> str:
        return f"{self.city} {self.nickname}"


@dataclass(frozen=True)
class Team:
    team_id: int
    seasons: Mapping[int, TeamSeason]

    @staticmethod
    def from_id(team_id: int) -> Team:
        assert team_id > 0, f"Invalid team_id: {team_id!r}"
        team = _TEAMS_BY_ID.get(team_id)
        assert team is not None, f"Unknown NBA team_id: {team_id!r}"
        return team

    @staticmethod
    def all() -> list[Team]:
        return [Team.from_id(team_id) for team_id in sorted(_TEAMS_BY_ID)]

    @staticmethod
    def from_abbreviation(
        abbreviation: str,
        *,
        season: Season | int | None = None,
        game_date: str | None = None,
    ) -> Team:
        assert abbreviation is not None and abbreviation.strip() != "", (
            "team abbreviation is required"
        )
        assert not (season is not None and game_date is not None), (
            "Use season or game_date, not both"
        )

        normalized = abbreviation.strip().upper()
        if season is not None:
            start_year = season if isinstance(season, int) else season.start_year
            span = _resolve_span_for_abbreviation_and_year(normalized, start_year)
            return Team.from_id(span.team_id)
        if game_date is not None:
            span = _resolve_span_for_abbreviation_and_year(
                normalized,
                _season_start_year_from_game_date(game_date),
            )
            return Team.from_id(span.team_id)

        lookup_abbreviation = _LOOKUP_ABBREVIATION_BY_ALIAS.get(normalized, normalized)
        team_id = _TEAM_ID_BY_LOOKUP_ABBREVIATION.get(lookup_abbreviation)
        assert team_id is not None, f"Unknown NBA team abbreviation: {abbreviation!r}"
        return Team.from_id(team_id)

    @staticmethod
    def all_active_in_season(season: Season | int) -> list[Team]:
        start_year = season if isinstance(season, int) else season.start_year
        return sorted(
            [team for team in _TEAMS_BY_ID.values() if start_year in team.seasons],
            key=lambda team: team.abbreviation(season=start_year),
        )

    def for_season(self, season: Season | int) -> TeamSeason:
        start_year = season if isinstance(season, int) else season.start_year
        team_season = self.seasons.get(start_year)
        assert team_season is not None, (
            f"Team id {self.team_id!r} was not active in season {start_year!r}"
        )
        return team_season

    def for_date(self, game_date: str) -> TeamSeason:
        return self.for_season(_season_start_year_from_game_date(game_date))

    def abbreviation(
        self,
        *,
        season: Season | int | None = None,
        game_date: str | None = None,
    ) -> str:
        assert not (season is not None and game_date is not None), (
            "Use season or game_date, not both"
        )
        if game_date is not None:
            return self.for_date(game_date).abbreviation
        if season is not None:
            return self.for_season(season).abbreviation
        raise AssertionError("Team.abbreviation requires season or game_date")

    def is_active_during(self, season: Season | int) -> bool:
        return (season if isinstance(season, int) else season.start_year) in self.seasons

    @property
    def current(self) -> TeamSeason:
        return self.seasons[max(self.seasons)]

    def validate(self):
        if self.team_id is None or self.team_id <= 0:
            raise ValueError(f"team {self} must have a positive team_id: {self.team_id}")


@dataclass(frozen=True)
class _TeamSpan:
    team_id: int
    abbreviation: str
    city: str
    nickname: str
    season_start: int
    season_end: int | None = None
    lookup_abbreviation: str | None = None

    def includes_year(self, season_start_year: int) -> bool:
        if season_start_year < self.season_start:
            return False
        return self.season_end is None or season_start_year <= self.season_end


_TEAM_SPANS: tuple[_TeamSpan, ...] = (
    _TeamSpan(1610612744, "PHW", "Philadelphia", "Warriors", 1946, 1961, "GSW"),
    _TeamSpan(1610612744, "SFW", "San Francisco", "Warriors", 1962, 1970, "GSW"),
    _TeamSpan(1610612744, "GSW", "Golden State", "Warriors", 1971),
    _TeamSpan(1610612738, "BOS", "Boston", "Celtics", 1946),
    _TeamSpan(1610612752, "NYK", "New York", "Knicks", 1946),
    _TeamSpan(1610612747, "MNL", "Minneapolis", "Lakers", 1948, 1959, "LAL"),
    _TeamSpan(1610612747, "LAL", "Los Angeles", "Lakers", 1960),
    _TeamSpan(1610612758, "ROC", "Rochester", "Royals", 1948, 1956, "SAC"),
    _TeamSpan(1610612758, "CIN", "Cincinnati", "Royals", 1957, 1971, "SAC"),
    _TeamSpan(1610612758, "KCO", "Kansas City-Omaha", "Kings", 1972, 1974, "SAC"),
    _TeamSpan(1610612758, "KCK", "Kansas City", "Kings", 1975, 1984, "SAC"),
    _TeamSpan(1610612758, "SAC", "Sacramento", "Kings", 1985),
    _TeamSpan(1610612765, "FTW", "Fort Wayne", "Pistons", 1948, 1956, "DET"),
    _TeamSpan(1610612765, "DET", "Detroit", "Pistons", 1957),
    _TeamSpan(1610612737, "TRI", "Tri-Cities", "Blackhawks", 1949, 1950, "ATL"),
    _TeamSpan(1610612737, "MLH", "Milwaukee", "Hawks", 1951, 1954, "ATL"),
    _TeamSpan(1610612737, "STL", "St. Louis", "Hawks", 1955, 1967, "ATL"),
    _TeamSpan(1610612737, "ATL", "Atlanta", "Hawks", 1968),
    _TeamSpan(1610612755, "SYR", "Syracuse", "Nationals", 1949, 1962, "PHI"),
    _TeamSpan(1610612755, "PHI", "Philadelphia", "76ers", 1963),
    _TeamSpan(1610612764, "CHP", "Chicago", "Packers", 1961, 1961, "WAS"),
    _TeamSpan(1610612764, "CHZ", "Chicago", "Zephyrs", 1962, 1962, "WAS"),
    _TeamSpan(1610612764, "BAL", "Baltimore", "Bullets", 1963, 1972, "WAS"),
    _TeamSpan(1610612764, "CAP", "Capital", "Bullets", 1973, 1973, "WAS"),
    _TeamSpan(1610612764, "WSB", "Washington", "Bullets", 1974, 1996, "WAS"),
    _TeamSpan(1610612764, "WAS", "Washington", "Wizards", 1997),
    _TeamSpan(1610612741, "CHI", "Chicago", "Bulls", 1966),
    _TeamSpan(1610612760, "SEA", "Seattle", "SuperSonics", 1967, 2007, "OKC"),
    _TeamSpan(1610612760, "OKC", "Oklahoma City", "Thunder", 2008),
    _TeamSpan(1610612745, "SDR", "San Diego", "Rockets", 1967, 1970, "HOU"),
    _TeamSpan(1610612745, "HOU", "Houston", "Rockets", 1971),
    _TeamSpan(1610612749, "MIL", "Milwaukee", "Bucks", 1968),
    _TeamSpan(1610612756, "PHX", "Phoenix", "Suns", 1968),
    _TeamSpan(1610612746, "BUF", "Buffalo", "Braves", 1970, 1977, "LAC"),
    _TeamSpan(1610612746, "SDC", "San Diego", "Clippers", 1978, 1983, "LAC"),
    _TeamSpan(1610612746, "LAC", "Los Angeles", "Clippers", 1984),
    _TeamSpan(1610612739, "CLE", "Cleveland", "Cavaliers", 1970),
    _TeamSpan(1610612757, "POR", "Portland", "Trail Blazers", 1970),
    _TeamSpan(1610612762, "NOJ", "New Orleans", "Jazz", 1974, 1978, "UTA"),
    _TeamSpan(1610612762, "UTA", "Utah", "Jazz", 1979),
    _TeamSpan(1610612743, "DEN", "Denver", "Nuggets", 1976),
    _TeamSpan(1610612754, "IND", "Indiana", "Pacers", 1976),
    _TeamSpan(1610612751, "NJN", "New Jersey", "Nets", 1976, 2011, "BKN"),
    _TeamSpan(1610612751, "BKN", "Brooklyn", "Nets", 2012),
    _TeamSpan(1610612759, "SAS", "San Antonio", "Spurs", 1976),
    _TeamSpan(1610612742, "DAL", "Dallas", "Mavericks", 1980),
    _TeamSpan(1610612766, "CHH", "Charlotte", "Hornets", 1988, 2001, "CHA"),
    _TeamSpan(1610612748, "MIA", "Miami", "Heat", 1988),
    _TeamSpan(1610612750, "MIN", "Minnesota", "Timberwolves", 1989),
    _TeamSpan(1610612753, "ORL", "Orlando", "Magic", 1989),
    _TeamSpan(1610612740, "NOH", "New Orleans", "Hornets", 2002, 2004, "NOP"),
    _TeamSpan(1610612740, "NOK", "New Orleans/Oklahoma City", "Hornets", 2005, 2006, "NOP"),
    _TeamSpan(1610612740, "NOH", "New Orleans", "Hornets", 2007, 2012, "NOP"),
    _TeamSpan(1610612740, "NOP", "New Orleans", "Pelicans", 2013),
    _TeamSpan(1610612766, "CHA", "Charlotte", "Bobcats", 2004, 2013),
    _TeamSpan(1610612766, "CHA", "Charlotte", "Hornets", 2014),
    _TeamSpan(1610612761, "TOR", "Toronto", "Raptors", 1995),
    _TeamSpan(1610612763, "VAN", "Vancouver", "Grizzlies", 1995, 2000, "MEM"),
    _TeamSpan(1610612763, "MEM", "Memphis", "Grizzlies", 2001),
)

_SPANS_BY_TEAM_ID: dict[int, list[_TeamSpan]] = {}
_SPANS_BY_ABBREVIATION: dict[str, list[_TeamSpan]] = {}
_LOOKUP_ABBREVIATION_BY_ALIAS: dict[str, str] = {}
_TEAM_ID_BY_LOOKUP_ABBREVIATION: dict[str, int] = {}

for span in _TEAM_SPANS:
    _SPANS_BY_TEAM_ID.setdefault(span.team_id, []).append(span)
    _SPANS_BY_ABBREVIATION.setdefault(span.abbreviation, []).append(span)
    _LOOKUP_ABBREVIATION_BY_ALIAS[span.abbreviation] = span.lookup_abbreviation or span.abbreviation

for spans in _SPANS_BY_ABBREVIATION.values():
    spans.sort(key=lambda span: (span.season_start, span.season_end or 9999))

for spans in _SPANS_BY_TEAM_ID.values():
    spans.sort(key=lambda span: (span.season_start, span.season_end or 9999))
    latest_span = max(spans, key=lambda span: (span.season_end or 9999, span.season_start))
    lookup_abbreviation = latest_span.lookup_abbreviation or latest_span.abbreviation
    _TEAM_ID_BY_LOOKUP_ABBREVIATION[lookup_abbreviation] = latest_span.team_id

_TEAMS_BY_ID: dict[int, Team] = {}

for team_id, spans in _SPANS_BY_TEAM_ID.items():
    seasons: dict[int, TeamSeason] = {}
    for span in spans:
        end_year = span.season_end or max(span.season_start, date.today().year)
        for start_year in range(span.season_start, end_year + 1):
            seasons[start_year] = TeamSeason(
                team_id=team_id,
                start_year=start_year,
                city=span.city,
                nickname=span.nickname,
                abbreviation=span.abbreviation,
            )
    _TEAMS_BY_ID[team_id] = Team(
        team_id=team_id,
        seasons=MappingProxyType(dict(sorted(seasons.items()))),
    )


def _resolve_span_for_abbreviation_and_year(abbreviation: str, season_start_year: int) -> _TeamSpan:
    spans = _SPANS_BY_ABBREVIATION.get(abbreviation)
    assert spans is not None, f"Unknown NBA team abbreviation: {abbreviation!r}"
    for span in spans:
        if span.includes_year(season_start_year):
            return span
    raise AssertionError(f"Team {abbreviation!r} was not active in season {season_start_year!r}")


def _season_start_year_from_game_date(game_date: str) -> int:
    parsed_date = date.fromisoformat(game_date)
    return parsed_date.year if parsed_date.month >= 7 else parsed_date.year - 1


def _team_label(team: Team | TeamSeason) -> str:
    if isinstance(team, TeamSeason):
        return f"{team.abbreviation} ({team.team_id})"
    return f"{team.current.abbreviation} ({team.team_id})"


def normalize_teams(teams: list[Team] | None) -> list[Team] | None:
    if not teams:
        return None
    unique_teams = {team.team_id: team for team in teams}
    return [unique_teams[team_id] for team_id in sorted(unique_teams)]


def to_normalized_team_ids(teams: list[Team] | None) -> list[int] | None:
    normalized_teams = normalize_teams(teams)
    if normalized_teams is None:
        return None
    return [team.team_id for team in normalized_teams]


def canonicalize_metric_team_filter(team_filter: str) -> str:
    if not team_filter:
        return ""

    team_ids = team_filter.split(",")
    if team_ids != sorted(set(team_ids), key=int):
        raise ValueError("team_filter must be unique and sorted")
    for team_id in team_ids:
        _canonical_team_id_filter_value(team_id)
    return ",".join(team_ids)


_TEAM_ID_FILTER_PATTERN = re.compile(r"^[1-9]\d*$")


def _canonical_team_id_filter_value(value: str) -> int:
    if not _TEAM_ID_FILTER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid team_filter value {value!r}")
    return int(value)
