from __future__ import annotations

import re

SEASON_PATTERN = re.compile(r"^(?P<start>\d{4})(?:-(?P<end>\d{2}))?$")


def format_season_string(start_year: int) -> str:
    if start_year < 0:
        raise ValueError(f"Invalid season start year: {start_year!r}")
    end_year = (start_year + 1) % 100
    return f"{start_year}-{end_year:02d}"


def canonicalize_season_string(value: str) -> str:
    season = value.strip()
    match = SEASON_PATTERN.fullmatch(season)
    if match is None:
        raise ValueError(
            f"Invalid season string {value!r}. Expected YYYY-YY or YYYY."
        )

    start_year = int(match.group("start"))
    end_year = match.group("end")
    canonical = format_season_string(start_year)
    if end_year is None:
        return canonical
    if season != canonical:
        raise ValueError(
            f"Invalid season string {value!r}. Expected canonical season {canonical!r}."
        )
    return canonical


def is_canonical_season_string(value: str) -> bool:
    try:
        return canonicalize_season_string(value) == value.strip()
    except ValueError:
        return False


def season_sort_key(value: str) -> int:
    canonical = canonicalize_season_string(value)
    return int(canonical[:4])

