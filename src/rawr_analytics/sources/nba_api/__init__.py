"""TODO: descrbe what this module does."""

from __future__ import annotations

from rawr_analytics.basketball.nba_api._load import (
    load_player_names_from_cache,
)
from rawr_analytics.basketball.nba_api.api import load_or_fetch_box_score
from rawr_analytics.basketball.nba_api.cache import (
    load_or_fetch_league_games,
)
from rawr_analytics.basketball.nba_api.dedupe import dedupe_schedule_games
from rawr_analytics.basketball.nba_api.models import (
    SourceBoxScore,
    SourceBoxScorePlayer,
    SourceBoxScoreTeam,
    SourceLeagueGame,
    SourceLeagueSchedule,
)
from rawr_analytics.basketball.nba_api.parsers import (
    parse_box_score_payload,
    parse_league_schedule_payload,
)

__all__ = [
    "SourceBoxScore",
    "SourceBoxScorePlayer",
    "SourceBoxScoreTeam",
    "SourceLeagueGame",
    "SourceLeagueSchedule",
    "dedupe_schedule_games",
    "load_or_fetch_box_score",
    "load_or_fetch_league_games",
    "load_player_names_from_cache",
    "parse_box_score_payload",
    "parse_league_schedule_payload",
]
