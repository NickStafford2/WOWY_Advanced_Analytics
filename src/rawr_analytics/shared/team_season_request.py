from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team, TeamSeason


@dataclass(frozen=True)
class TeamSeasonRequest:
    team: Team
    season: Season
    cached_only: bool = False

    @property
    def team_season(self) -> TeamSeason:
        return self.team.for_season(self.season)

    @property
    def team_id(self) -> int:
        return self.team_season.team_id

    @property
    def team_abbreviation(self) -> str:
        return self.team_season.abbreviation

    @property
    def season_id(self) -> str:
        return self.season.id

    @property
    def season_type_label(self) -> SeasonType:
        return self.season.season_type

    @property
    def scope_label(self) -> str:
        return f"{self.team_abbreviation} {self.season_id} {self.season_type_label}"
