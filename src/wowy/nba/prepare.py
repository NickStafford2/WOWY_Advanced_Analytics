from __future__ import annotations

from pathlib import Path

from wowy.apps.wowy.derive import derive_wowy_games
from wowy.apps.wowy.models import WowyGameRecord
from wowy.data.game_cache.repository import (
    load_cache_load_row,
    load_normalized_game_players_from_db,
    load_normalized_games_from_db,
)
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.team_seasons import TeamSeasonScope, resolve_team_seasons


def load_wowy_game_records(
    teams: list[str] | None,
    seasons: list[str] | None,
    *,
    team_ids: list[int] | None = None,
    season_type: str = "Regular Season",
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> tuple[list[WowyGameRecord], dict[int, str]]:
    games, game_players = load_normalized_scope_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
    )
    player_names = {player.player_id: player.player_name for player in game_players}
    return derive_wowy_games(games, game_players), player_names


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
    for team_season in requested_team_seasons:
        _require_cached_team_season_scope(
            team_season,
            season_type=season_type,
            player_metrics_db_path=player_metrics_db_path,
        )

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
            _require_cached_team_season_scope(
                team_season,
                season_type=season_type,
                player_metrics_db_path=player_metrics_db_path,
            )
            team_seasons.append(team_season)

    games = load_normalized_games_from_db(
        player_metrics_db_path,
        season_type=season_type,
        teams=[team_season.team for team_season in team_seasons],
        seasons=sorted({team_season.season for team_season in team_seasons}),
    )
    game_players = load_normalized_game_players_from_db(
        player_metrics_db_path,
        season_type=season_type,
        teams=[team_season.team for team_season in team_seasons],
        seasons=sorted({team_season.season for team_season in team_seasons}),
    )
    games, game_players = _filter_records_to_team_seasons(
        games,
        game_players,
        team_seasons,
    )
    if games and game_players:
        return games, game_players
    raise ValueError("No database cache matched the requested scope")


def _load_normalized_games_from_db_for_scope(
    team_seasons: list[TeamSeasonScope],
    *,
    season_type: str,
    player_metrics_db_path: Path,
) -> list[NormalizedGameRecord]:
    games = load_normalized_games_from_db(
        player_metrics_db_path,
        season_type=season_type,
        teams=[team_season.team for team_season in team_seasons],
        seasons=sorted({team_season.season for team_season in team_seasons}),
    )
    games, _game_players = _filter_records_to_team_seasons(
        games,
        [],
        team_seasons,
    )
    if games:
        return games
    raise ValueError("No database cache matched the requested scope")


def _filter_records_to_team_seasons(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    team_seasons: list[TeamSeasonScope],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    allowed_team_seasons = {
        (team_season.team_id, team_season.season) for team_season in team_seasons
    }
    filtered_games = [
        game
        for game in games
        if (game.identity_team, game.season) in allowed_team_seasons
    ]
    allowed_game_teams = {(game.game_id, game.identity_team) for game in filtered_games}
    filtered_game_players = [
        player
        for player in game_players
        if (player.game_id, player.identity_team) in allowed_game_teams
    ]
    return filtered_games, filtered_game_players


def _require_cached_team_season_scope(
    team_season: TeamSeasonScope,
    *,
    season_type: str,
    player_metrics_db_path: Path,
) -> None:
    cache_load_row = load_cache_load_row(
        player_metrics_db_path,
        team=team_season.team,
        season=team_season.season,
        season_type=season_type,
    )
    if (
        cache_load_row is not None
        and cache_load_row.games_row_count > 0
        and cache_load_row.game_players_row_count > 0
    ):
        return
    raise ValueError(
        f"Missing cached team-season scope for {team_season.team} "
        f"{team_season.season} {season_type}"
    )
