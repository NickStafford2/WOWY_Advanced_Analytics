from __future__ import annotations

from rawr_analytics.data.game_cache import load_normalized_scope_records_from_db
from rawr_analytics.data.game_cache.rows import NormalizedGamePlayerRow, NormalizedGameRow
from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    WowyPlayerValue,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.inputs import build_wowy_season_inputs
from rawr_analytics.metrics.wowy.records import (
    WowyPlayerSeasonRecord,
    WowyPlayerSeasonValue,
    prepare_wowy_player_season_records,
)
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def load_wowy_records(
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    team_seasons = resolve_team_seasons(teams, seasons, season_type=season_type)
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")

    game_rows, game_player_rows = load_normalized_scope_records_from_db(team_seasons)
    games = [_build_normalized_game_record(row) for row in game_rows]
    game_players = [_build_normalized_game_player_record(row) for row in game_player_rows]
    return games, game_players


def build_wowy_store_rows(
    *,
    metric: Metric,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    teams: list[Team] | None,
) -> list[WowyPlayerSeasonValueRow]:
    games, game_players = load_wowy_records(
        teams=teams,
        seasons=None,
        season_type=season_type,
    )
    season_inputs = build_wowy_season_inputs(games=games, game_players=game_players)
    records = prepare_wowy_player_season_records(
        season_inputs=season_inputs,
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return [
        _build_wowy_store_row(
            metric=metric,
            record=record,
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
        )
        for record in records
    ]


def build_wowy_value_from_store_row(row: WowyPlayerSeasonValueRow) -> WowyPlayerSeasonValue:
    return WowyPlayerSeasonValue(
        season_id=row.season_id,
        player=PlayerSummary(
            player_id=row.player_id,
            player_name=row.player_name,
        ),
        minutes=PlayerMinutes(
            average_minutes=row.average_minutes,
            total_minutes=row.total_minutes,
        ),
        result=WowyPlayerValue(
            games_with=row.games_with,
            games_without=row.games_without,
            avg_margin_with=row.avg_margin_with,
            avg_margin_without=row.avg_margin_without,
            value=row.value,
            raw_value=row.raw_wowy_score,
        ),
    )


def _build_wowy_store_row(
    *,
    metric: Metric,
    record: WowyPlayerSeasonRecord,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
) -> WowyPlayerSeasonValueRow:
    value = record.result.value
    include_raw_wowy_score = False
    if metric == Metric.WOWY_SHRUNK:
        include_raw_wowy_score = True
        value = compute_wowy_shrinkage_score(
            games_with=record.result.games_with,
            games_without=record.result.games_without,
            wowy_score=record.result.value,
            prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
        )
    return WowyPlayerSeasonValueRow(
        snapshot_id=None,
        metric_id=metric.value,
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type.value,
        season_id=record.season.id,
        player_id=record.player.player_id,
        player_name=record.player.player_name,
        value=value,
        games_with=record.result.games_with,
        games_without=record.result.games_without,
        avg_margin_with=record.result.avg_margin_with,
        avg_margin_without=record.result.avg_margin_without,
        average_minutes=record.minutes.average_minutes,
        total_minutes=record.minutes.total_minutes,
        raw_wowy_score=record.result.value if include_raw_wowy_score else None,
    )


def _build_normalized_game_record(row: NormalizedGameRow) -> NormalizedGameRecord:
    return NormalizedGameRecord(
        game_id=row.game_id,
        game_date=row.game_date,
        season=row.season,
        team=row.team,
        opponent_team=row.opponent_team,
        is_home=row.is_home,
        margin=row.margin,
        source=row.source,
    )


def _build_normalized_game_player_record(
    row: NormalizedGamePlayerRow,
) -> NormalizedGamePlayerRecord:
    return NormalizedGamePlayerRecord(
        game_id=row.game_id,
        player=row.player,
        appeared=row.appeared,
        minutes=row.minutes,
        team=row.team,
    )
