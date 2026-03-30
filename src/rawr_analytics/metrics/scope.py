from __future__ import annotations

from rawr_analytics.metrics._scope_values import team_ids
from rawr_analytics.shared.season import SeasonType
from rawr_analytics.shared.team import Team


def build_team_filter(teams: list[Team] | None) -> str:
    normalized_team_ids = team_ids(teams) or []
    return ",".join(str(team_id) for team_id in normalized_team_ids)


def build_scope_key(
    season_type: SeasonType,
    team_filter: str = "all-teams",
) -> str:
    return f"team_ids={team_filter}|season_type={season_type.value}"
