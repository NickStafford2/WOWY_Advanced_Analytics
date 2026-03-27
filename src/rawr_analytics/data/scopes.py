from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class TeamSeasonScope:
    season: str
    team_id: int
