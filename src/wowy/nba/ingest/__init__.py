from __future__ import annotations

from wowy.nba.ingest.cache import DEFAULT_SOURCE_DATA_DIR


__all__ = [
    "DEFAULT_SOURCE_DATA_DIR",
    "cache_team_season_data",
    "fetch_team_season_data",
    "ingest_team_season",
    "load_player_names_from_cache",
    "season_type_slug",
]


def __getattr__(name: str):
    if name in {
        "cache_team_season_data",
        "fetch_team_season_data",
        "ingest_team_season",
        "load_player_names_from_cache",
        "season_type_slug",
    }:
        from wowy.nba.ingest import runner

        return getattr(runner, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
