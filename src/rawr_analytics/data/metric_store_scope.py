from __future__ import annotations

from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team, normalize_teams, to_team_ids

__all__ = [
    "build_scope_key",
    "build_team_filter",
    "season_ids",
]


def build_team_filter(teams: list[Team] | None) -> str:
    normalized_team_ids = to_team_ids(normalize_teams(teams)) or []
    return ",".join(str(team_id) for team_id in normalized_team_ids)


def build_scope_key(
    *,
    season_type: SeasonType,
    teams: list[Team] | None = None,
    team_filter: str | None = None,
) -> str:
    resolved_team_filter = team_filter if team_filter is not None else build_team_filter(teams)
    return f"team_ids={resolved_team_filter or 'all-teams'}|season_type={season_type.value}"


def season_ids(seasons: list[Season] | None) -> list[str] | None:
    if not seasons:
        return None
    return sorted({season.id for season in seasons})
