from __future__ import annotations


def format_scope(teams: list[str] | None, seasons: list[str] | None) -> str:
    team_label = (
        ",".join(team.upper() for team in teams) if teams else "all cached teams"
    )
    season_label = ",".join(seasons) if seasons else "all cached seasons"
    return f"teams={team_label} seasons={season_label}"
