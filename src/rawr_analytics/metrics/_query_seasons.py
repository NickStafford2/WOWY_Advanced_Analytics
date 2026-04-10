# from __future__ import annotations
#
# from rawr_analytics.shared.season import (
#     Season,
#     SeasonType,
#     build_all_nba_history_seasons,
#     normalize_seasons,
# )
# from rawr_analytics.shared.team import Team, normalize_teams


# don't think i need this. if team has no seasons in span, the db will take care of it.
# def resolve_query_seasons(
#     *,
#     teams: list[Team] | None,
#     season_types: list[SeasonType] | None,
#     # season_type: SeasonType,
# ) -> list[Season]:
#     normalized_season_filter = normalize_seasons(season_filter)
#     if normalized_season_filter is not None:
#         # _validate_query_season_type(
#         #     seasons=normalized_season_filter,
#         #     # season_type=season_type,
#         # )
#         # assert normalized_season_filter, "season_filter normalization produced no seasons"
#         return normalized_season_filter
#
#     normalized_teams = normalize_teams(teams)
#     history_seasons = build_all_nba_history_seasons(season_types=season_types)
#     if normalized_teams is None:
#         return history_seasons
#
#     team_history_seasons = [
#         season
#         for season in history_seasons
#         if any(team.is_active_during(season) for team in normalized_teams)
#     ]
#     assert team_history_seasons, "team_filter resolved to no active NBA seasons"
#     return team_history_seasons


# def _validate_query_season_type(
#     *,
#     seasons: list[Season],
#     season_type: SeasonType,
# ) -> None:
#     invalid_seasons = [season.id for season in seasons if season.season_type != season_type]
#     assert not invalid_seasons, (
#         "Mixed season types are not supported by the current metric query boundary: "
#         f"{invalid_seasons!r}"
#     )
