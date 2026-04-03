"""Public API for the nba_api source adapter."""

from __future__ import annotations

from rawr_analytics.sources.nba_api._load import (
    load_player_names_from_cache,
)
from rawr_analytics.sources.nba_api.api import (
    NbaApiGameIngestUpdate,
    NbaApiTeamSeasonData,
    ingest_team_season,
    load_or_fetch_box_score,
)
from rawr_analytics.sources.nba_api.cache import (
    load_or_fetch_league_games,
)
from rawr_analytics.sources.nba_api.dedupe import dedupe_schedule_games
from rawr_analytics.sources.nba_api.models import (
    SourceBoxScore,
    SourceBoxScorePlayer,
    SourceBoxScoreTeam,
    SourceLeagueGame,
    SourceLeagueSchedule,
)
from rawr_analytics.sources.nba_api.normalize import normalize_source_league_game
from rawr_analytics.sources.nba_api.parsers import (
    parse_box_score_payload,
    parse_league_schedule_payload,
)

__all__ = [
    "SourceBoxScore",
    "SourceBoxScorePlayer",
    "SourceBoxScoreTeam",
    "SourceLeagueGame",
    "SourceLeagueSchedule",
    "NbaApiGameIngestUpdate",
    "NbaApiTeamSeasonData",
    "dedupe_schedule_games",
    "ingest_team_season",
    "load_or_fetch_box_score",
    "load_or_fetch_league_games",
    "load_player_names_from_cache",
    "normalize_source_league_game",
    "parse_box_score_payload",
    "parse_league_schedule_payload",
]
