from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WowyGameRecord:
    game_id: str
    season: str
    team: str
    margin: float
    players: set[int]


@dataclass(frozen=True)
class NormalizedGameRecord:
    game_id: str
    season: str
    game_date: str
    team: str
    opponent: str
    is_home: bool
    margin: float
    season_type: str
    source: str


@dataclass(frozen=True)
class NormalizedGamePlayerRecord:
    game_id: str
    team: str
    player_id: int
    player_name: str
    appeared: bool
    minutes: float | None


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


@dataclass(frozen=True)
class WowyPlayerStats:
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    wowy_score: float | None
    average_minutes: float | None = None
    total_minutes: float | None = None
