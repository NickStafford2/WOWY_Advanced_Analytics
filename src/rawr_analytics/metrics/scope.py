from __future__ import annotations

from rawr_analytics.shared.season import SeasonType


def build_team_filter(team_ids: list[int] | None) -> str:
    normalized_team_ids = sorted({team_id for team_id in team_ids or [] if team_id > 0})
    return ",".join(str(team_id) for team_id in normalized_team_ids)


def build_scope_key(
    season_type: SeasonType,
    team_filter: str = "all-teams",
) -> str:
    return f"team_ids={team_filter}|season_type={season_type.value}"
