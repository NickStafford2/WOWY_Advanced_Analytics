from __future__ import annotations

from dataclasses import dataclass

from wowy.apps.wowy.models import WowyGameRecord
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord


@dataclass(frozen=True)
class TeamSeasonArtifacts:
    normalized_games: list[NormalizedGameRecord]
    normalized_game_players: list[NormalizedGamePlayerRecord]
    wowy_games: list[WowyGameRecord]


@dataclass(frozen=True)
class TeamSeasonRunSummary:
    team: str
    season: str
    season_type: str
    league_games_source: str
    total_games: int
    processed_games: int
    skipped_games: int
    fetched_box_scores: int
    cached_box_scores: int


@dataclass(frozen=True)
class TeamSeasonBuildResult:
    artifacts: TeamSeasonArtifacts
    summary: TeamSeasonRunSummary
