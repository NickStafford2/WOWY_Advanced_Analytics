from __future__ import annotations


_SEASON_TYPE_ALIASES = {
    "playoff": "Playoffs",
    "playoffs": "Playoffs",
    "post season": "Playoffs",
    "postseason": "Playoffs",
    "regular": "Regular Season",
    "regular season": "Regular Season",
    "reg season": "Regular Season",
    "reg. season": "Regular Season",
}


def canonicalize_season_type(value: str) -> str:
    season_type = value.strip()
    if not season_type:
        raise ValueError("Season type must not be empty")

    canonical = _SEASON_TYPE_ALIASES.get(season_type.casefold())
    if canonical is None:
        raise ValueError(
            f"Invalid season type {value!r}. Expected 'Regular Season' or 'Playoffs'."
        )
    return canonical
