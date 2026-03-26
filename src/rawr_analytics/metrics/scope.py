from __future__ import annotations

from rawr_analytics.nba.season_types import canonicalize_season_type


def build_scope_key(
    *,
    team_ids: list[int] | None,
    season_type: str,
) -> tuple[str, str]:
    season_type = canonicalize_season_type(season_type)
    normalized_team_ids = sorted({team_id for team_id in team_ids or [] if team_id > 0})
    team_filter = ",".join(str(team_id) for team_id in normalized_team_ids)
    team_key = team_filter or "all-teams"
    return (
        f"team_ids={team_key}|season_type={season_type}",
        team_filter,
    )
