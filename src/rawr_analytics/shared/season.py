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
        raise AssertionError(
            f"Invalid season type {value!r}. Expected 'Regular Season', 'Playoffs', or 'Preseason'."
        )

    def to_nba_format(self) -> str:
        if self == SeasonType.REGULAR:
            return "Regular Season"
        if self == SeasonType.PLAYOFFS:
            return "Playoffs"
        if self == SeasonType.PRESEASON:
            return "Pre Season"
        raise AssertionError(f"Unsupported season type: {self!r}")

    def to_slug(self) -> str:
        if self == SeasonType.REGULAR:
            return "regular_season"
        if self == SeasonType.PLAYOFFS:
            return "playoffs"
        if self == SeasonType.PRESEASON:
            return "preseason"
        raise AssertionError(f"Unsupported season type: {self!r}")


@dataclass(frozen=True)
class Season:
    start_year: int
    season_type: SeasonType

    @property
    def id(self) -> str:  # "2014-15"
        return f"{self.start_year}-{(self.start_year + 1) % 100:02d}"

    @classmethod
    def parse(cls, year_string: str, season_type_str: str) -> Season:
        if not year_string or not year_string.strip():
            raise ValueError("season is required")

        if not season_type_str or not season_type_str.strip():
            raise ValueError("season_type is required")

        start_year = cls._parse_start_year(year_string)
        season_type = SeasonType.parse(season_type_str)

        return cls(start_year=start_year, season_type=season_type)

    def is_playoffs(self) -> bool:
        return self.season_type == SeasonType.PLAYOFFS

    def __str__(self) -> str:
        return self.id

    def to_nba_api_format(self) -> str:
        return self.id

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

    @staticmethod
    def are_same(
        left: Season,
        right: Season,
    ) -> bool:
        return (
            left.start_year == right.start_year
            and left.season_type.value == right.season_type.value
            and left.id == right.id
        )


def build_season_list(start_year: int, first_year: int, season_type_str: str) -> list[Season]:
    assert start_year >= first_year, "Start year must be greater than or equal to end year"
    return [
        Season.parse(str(year), season_type_str) for year in range(start_year, first_year - 1, -1)
    ]
