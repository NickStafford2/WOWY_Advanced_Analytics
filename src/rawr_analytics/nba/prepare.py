from __future__ import annotations

from pathlib import Path

from rawr_analytics.data.game_cache.repository import (
    load_normalized_scope_records_from_db,
)
from rawr_analytics.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from rawr_analytics.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.nba.team_seasons import TeamSeasonScope, resolve_team_seasons


def load_normalized_scope_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    season_type: str = "Regular Season",
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    include_opponents_for_team_scope: bool = True,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        team_ids=team_ids,
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")

    requested_team_seasons = list(team_seasons)

    if (teams or team_ids) and include_opponents_for_team_scope:
        opponent_team_seasons = {
            TeamSeasonScope(
                team=game.opponent,
                team_id=game.opponent_team_id,
                season=game.season,
            )
            for game in _load_normalized_games_from_db_for_scope(
                requested_team_seasons,
                season_type=season_type,
                player_metrics_db_path=player_metrics_db_path,
            )
        }
        for team_season in sorted(opponent_team_seasons):
            if team_season in requested_team_seasons:
                continue
            team_seasons.append(team_season)

    return load_normalized_scope_records_from_db(
        player_metrics_db_path,
        team_seasons=team_seasons,
        season_type=season_type,
    )


def _load_normalized_games_from_db_for_scope(
    team_seasons: list[TeamSeasonScope],
    *,
    season_type: str,
    player_metrics_db_path: Path,
) -> list[NormalizedGameRecord]:
    games, _game_players = load_normalized_scope_records_from_db(
        player_metrics_db_path,
        team_seasons=team_seasons,
        season_type=season_type,
    )
    return games
