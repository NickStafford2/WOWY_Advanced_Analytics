from __future__ import annotations

from rawr_analytics.data.game_cache import load_team_season_cache
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics.wowy.inputs import build_wowy_season_inputs
from rawr_analytics.metrics.wowy.records import (
    WowyPlayerSeasonRecord,
    prepare_wowy_player_season_records,
)
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def load_wowy_records(
    *,
    teams: list[Team] | None,
    seasons: list[Season],
    season_type: SeasonType,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    assert seasons, "WOWY record loading requires a non-empty season list"
    team_seasons = resolve_team_seasons(teams, seasons, season_type=season_type)
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")
    return load_team_season_cache(team_seasons)


def build_wowy_refresh_records(
    *,
    season_type: SeasonType,
    seasons: list[Season],
    teams: list[Team] | None,
) -> list[WowyPlayerSeasonRecord]:
    assert seasons, "WOWY refresh record builds require explicit non-empty seasons"
    games, game_players = load_wowy_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
    )
    season_inputs = build_wowy_season_inputs(games=games, game_players=game_players)
    return prepare_wowy_player_season_records(
        season_inputs=season_inputs,
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
