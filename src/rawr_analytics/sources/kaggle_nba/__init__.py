"""Public API for the Kaggle NBA dataset source adapter."""

from __future__ import annotations

from rawr_analytics.sources.kaggle_nba.api import (
    KaggleNbaTeamSeasonData,
    ingest_team_season,
)

__all__ = ["KaggleNbaTeamSeasonData", "ingest_team_season"]
