from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

_SEASON_YEAR_PATTERN = re.compile(r"^(?P<start>\d{4})(?:-(?P<end>\d{2}))?$")


class SeasonType(Enum):
    REGULAR = "REGULAR"
    PLAYOFFS = "PLAYOFFS"
    PRESEASON = "PRESEASON"

    @staticmethod
    def parse(value: str) -> SeasonType:
        season_type = value.strip().lower()
        if season_type in ["playoffs", "playoff", "post season", "post", "postseason"]:
            return SeasonType.PLAYOFFS
        if season_type in ["regular season", "regular", "reg season", "reg. season"]:
            return SeasonType.REGULAR
        if season_type in ["pre", "preseason", "pre season"]:
            return SeasonType.PRESEASON
        assert False, (
            f"Invalid season type {value!r}. "
            "Expected 'Regular Season', 'Playoffs', or 'Preseason'."
        )


@dataclass
class Season:
    start_year: int
    id: str  # "2014-15"
    season_type: SeasonType

    def __init__(self, year_string: str, season_type_str: str):
        assert season_type_str is not None and season_type_str != "", "season_type is required"
        assert year_string is not None and year_string != "", "season is required"
        self.start_year = Season._parse_start_year(year_string)
        self.season_type = SeasonType.parse(season_type_str)
        self.id = Season._to_nba_api_format(self.start_year)

    def is_playoffs(self) -> bool:
        return self.season_type == SeasonType.PLAYOFFS

    @staticmethod
    def _parse_start_year(year_string: str) -> int:
        season = year_string.strip()
        reg_exp_match = _SEASON_YEAR_PATTERN.fullmatch(season)
        assert reg_exp_match is not None, (
            f"Invalid season string {year_string!r}. Expected YYYY-YY or YYYY."
        )

        start_year = int(reg_exp_match.group("start"))
        assert start_year > 0, f"Invalid season start year: {start_year!r}"
        end_suffix = reg_exp_match.group("end")
        if end_suffix is not None:
            expected_end_suffix = f"{(start_year + 1) % 100:02d}"
            assert end_suffix == expected_end_suffix, (
                f"Invalid season string {year_string!r}. "
                f"Expected {start_year}-{expected_end_suffix}."
            )
        return start_year

    @staticmethod
    def _to_nba_api_format(start_year: int) -> str:
        assert start_year >= 0, f"Invalid season start year: {start_year!r}"
        end_year = (start_year + 1) % 100
        return f"{start_year}-{end_year:02d}"
