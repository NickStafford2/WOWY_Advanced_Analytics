from __future__ import annotations

from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team, canonicalize_metric_team_filter, to_normalized_team_ids

__all__ = [
    "build_scope_key",
    "build_team_filter",
    "season_ids",
    "validate_metric_scope",
]

# TODO: Replace the single season_type scope boundary with an exact canonical
# ordered season list in the scope key so metric snapshots can represent mixed
# regular-season and playoff queries.
#
# Canonical rule going forward:
# - scope identity will be team_filter + exact ordered seasons
# - seasons will be normalized before key construction
# - normalization will dedupe exact Season values
# - ordering will be by (season.id, season.season_type.value)
# - encoded season values will stay explicit rather than hashed so the key is
#   readable and directly debuggable
# - validation will compare against that canonical encoded season list rather
#   than a single season_type field


def build_team_filter(teams: list[Team] | None) -> str:
    team_ids = to_normalized_team_ids(teams) or []
    return ",".join(str(team_id) for team_id in team_ids)


def build_scope_key(
    *,
    season_type: SeasonType,
    teams: list[Team] | None = None,
    team_filter: str | None = None,
) -> str:
    resolved_team_filter = team_filter if team_filter is not None else build_team_filter(teams)
    return f"team_ids={resolved_team_filter or 'all-teams'}|season_type={season_type.value}"


def validate_metric_scope(*, scope_key: str, team_filter: str, season_type: str) -> None:
    canonical_season_type = SeasonType.parse(season_type)
    canonical_team_filter = canonicalize_metric_team_filter(team_filter)
    expected_scope_key = build_scope_key(
        season_type=canonical_season_type,
        team_filter=canonical_team_filter,
    )
    if scope_key != expected_scope_key:
        raise ValueError(
            f"Invalid scope_key {scope_key!r}; expected canonical {expected_scope_key!r}"
        )


def season_ids(seasons: list[Season] | None) -> list[str] | None:
    if not seasons:
        return None
    return sorted({season.year_string_nba_api for season in seasons})
