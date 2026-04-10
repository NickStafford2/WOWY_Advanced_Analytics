from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.calculate._analysis import (
    WowyPlayerValue,
    compute_wowy,
    filter_results,
)
from rawr_analytics.metrics.wowy.calculate.inputs import (
    WowyRequestDTO,
    WowySeasonInputDTO,
    build_wowy_request,
    validate_request,
)
from rawr_analytics.metrics.wowy.calculate.shrinkage import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class WowyPlayerSeasonValue:
    season: Season
    player: PlayerSummary
    minutes: PlayerMinutes
    result: WowyPlayerValue


@dataclass(frozen=True)
class WowyPlayerSeasonRecord:
    season: Season
    player: PlayerSummary
    minutes: PlayerMinutes
    result: WowyPlayerValue


def build_player_season_records(request: WowyRequestDTO) -> list[WowyPlayerSeasonRecord]:
    validate_request(request)
    records: list[WowyPlayerSeasonRecord] = []
    for season_input in sorted(
        request.season_inputs, key=lambda item: item.season.year_string_nba_api
    ):
        records.extend(_build_season_records(season_input, request=request))
    records.sort(
        key=lambda record: (
            record.season.year_string_nba_api,
            record.result.value if record.result.value is not None else float("-inf"),
            record.player.player_name,
        ),
        reverse=True,
    )
    return records


def _build_season_records(
    season_input: WowySeasonInputDTO,
    *,
    request: WowyRequestDTO,
) -> list[WowyPlayerSeasonRecord]:
    results = compute_wowy(season_input.games)
    filtered_results = filter_results(
        results,
        min_games_with=request.min_games_with,
        min_games_without=request.min_games_without,
    )

    records: list[WowyPlayerSeasonRecord] = []
    ranked_results = sorted(
        filtered_results.items(),
        key=lambda item: item[1].value if item[1].value is not None else float("-inf"),
        reverse=True,
    )
    for player_id, value in ranked_results:
        player = season_input.players_by_id[player_id]
        if not player.passes_minute_filters(
            min_average_minutes=request.min_average_minutes,
            min_total_minutes=request.min_total_minutes,
        ):
            continue
        assert value.avg_margin_with is not None
        assert value.avg_margin_without is not None
        assert value.value is not None
        records.append(
            WowyPlayerSeasonRecord(
                season=season_input.season,
                player=player.player,
                minutes=player.minutes,
                result=value,
            )
        )
    return records


def prepare_wowy_player_season_records(
    *,
    season_inputs: list[WowySeasonInputDTO],
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[WowyPlayerSeasonRecord]:
    return build_player_season_records(
        build_wowy_request(
            season_inputs=season_inputs,
            min_games_with=min_games_with,
            min_games_without=min_games_without,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    )


def build_wowy_custom_query(
    metric: Metric,
    *,
    season_inputs: list[WowySeasonInputDTO],
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> list[WowyPlayerSeasonValue]:
    records = prepare_wowy_player_season_records(
        season_inputs=season_inputs,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return [_build_wowy_query_row(metric, record) for record in records]


def build_wowy_player_season_value(
    *,
    season_id: str,
    player: PlayerSummary,
    minutes: PlayerMinutes,
    games_with: int,
    games_without: int,
    avg_margin_with: float | None,
    avg_margin_without: float | None,
    value: float | None,
    raw_value: float | None,
) -> WowyPlayerSeasonValue:
    return WowyPlayerSeasonValue(
        season_id=season_id,
        player=player,
        minutes=minutes,
        result=WowyPlayerValue(
            games_with=games_with,
            games_without=games_without,
            avg_margin_with=avg_margin_with,
            avg_margin_without=avg_margin_without,
            value=value,
            raw_value=raw_value,
        ),
    )


def _build_wowy_query_row(
    metric: Metric,
    record: WowyPlayerSeasonRecord,
) -> WowyPlayerSeasonValue:
    if metric == Metric.WOWY:
        return WowyPlayerSeasonValue(
            season_id=record.season.year_string_nba_api,
            player=record.player,
            minutes=record.minutes,
            result=WowyPlayerValue(
                games_with=record.result.games_with,
                games_without=record.result.games_without,
                avg_margin_with=record.result.avg_margin_with,
                avg_margin_without=record.result.avg_margin_without,
                value=record.result.value,
                raw_value=None,
            ),
        )
    if metric == Metric.WOWY_SHRUNK:
        return WowyPlayerSeasonValue(
            season_id=record.season.year_string_nba_api,
            player=record.player,
            minutes=record.minutes,
            result=WowyPlayerValue(
                games_with=record.result.games_with,
                games_without=record.result.games_without,
                avg_margin_with=record.result.avg_margin_with,
                avg_margin_without=record.result.avg_margin_without,
                value=compute_wowy_shrinkage_score(
                    games_with=record.result.games_with,
                    games_without=record.result.games_without,
                    wowy_score=record.result.value,
                    prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
                ),
                raw_value=record.result.raw_value,
            ),
        )
    raise ValueError(f"Unknown WOWY metric: {metric}")
