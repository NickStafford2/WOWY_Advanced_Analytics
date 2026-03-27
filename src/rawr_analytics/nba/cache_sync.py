# from __future__ import annotations
#
# from pathlib import Path
# from typing import Callable
#
# from rawr_analytics.data.game_cache import has_cached_team_season_scope
# from rawr_analytics.data.scopes import TeamSeasonScope
# from rawr_analytics.nba.source.cache import DEFAULT_SOURCE_DATA_DIR
# from rawr_analytics.workflows.nba_ingest import refresh_normalized_team_season_cache
#
# LogFn = Callable[[str], None]
#
#
# def ensure_team_season_data(
#     team_season: TeamSeasonScope,
#     season_type: str = "Regular Season",
#     source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
#     log: LogFn | None = print,
# ) -> None:
#     if has_cached_team_season_scope(
#         team_id=team_season.team_id,
#         season=team_season.season,
#         season_type=season_type,
#     ):
#         return
#     if log is not None:
#         log(f"fetch {team_season.team_id} {team_season.season}")
#     refresh_normalized_team_season_cache(
#         team_abbreviation=team_season.team,
#         season=team_season.season,
#         season_type=season_type,
#         source_data_dir=source_data_dir,
#         log=log,
#     )
