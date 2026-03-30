from __future__ import annotations

from rawr_analytics.metrics.wowy._analysis import compute_wowy, filter_results
from rawr_analytics.metrics.wowy._inputs import passes_minute_filters
from rawr_analytics.metrics.wowy.models import WowyPlayerSeasonRecord, WowyRequest, WowySeasonInput


def build_player_season_records(request: WowyRequest) -> list[WowyPlayerSeasonRecord]:
    records: list[WowyPlayerSeasonRecord] = []
    for season_input in sorted(request.season_inputs, key=lambda item: item.season.id):
        records.extend(_build_season_records(season_input, request=request))
    return records


def _build_season_records(
    season_input: WowySeasonInput,
    *,
    request: WowyRequest,
) -> list[WowyPlayerSeasonRecord]:
    player_contexts = {player.player_id: player for player in season_input.players}
    results = compute_wowy(season_input.games)
    filtered_results = filter_results(
        results,
        min_games_with=request.min_games_with,
        min_games_without=request.min_games_without,
    )

    records: list[WowyPlayerSeasonRecord] = []
    ranked_results = sorted(
        filtered_results.items(),
        key=lambda item: item[1].wowy_score if item[1].wowy_score is not None else float("-inf"),
        reverse=True,
    )
    for player_id, value in ranked_results:
        player = player_contexts[player_id]
        if not passes_minute_filters(
            player,
            min_average_minutes=request.min_average_minutes,
            min_total_minutes=request.min_total_minutes,
        ):
            continue
        assert value.avg_margin_with is not None
        assert value.avg_margin_without is not None
        assert value.wowy_score is not None
        records.append(
            WowyPlayerSeasonRecord(
                season=season_input.season,
                player_id=player.player_id,
                player_name=player.player_name,
                games_with=value.games_with,
                games_without=value.games_without,
                avg_margin_with=value.avg_margin_with,
                avg_margin_without=value.avg_margin_without,
                wowy_score=value.wowy_score,
                average_minutes=player.average_minutes,
                total_minutes=player.total_minutes,
            )
        )
    return records
