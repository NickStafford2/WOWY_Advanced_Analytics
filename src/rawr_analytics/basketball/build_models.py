from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.basketball.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope


@dataclass(frozen=True)
class TeamSeasonArtifacts:
    normalized_games: list[NormalizedGameRecord]
    normalized_game_players: list[NormalizedGamePlayerRecord]


@dataclass(frozen=True)
class TeamSeasonRunSummary:
    scope: TeamSeasonScope
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
