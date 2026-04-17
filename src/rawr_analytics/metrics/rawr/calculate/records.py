# src/rawr_analytics/metrics/rawr/calculate/records.py

from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.rawr.calculate._observations import count_player_season_games
from rawr_analytics.metrics.rawr.calculate._regression import fit_player_rawr
from rawr_analytics.metrics.rawr.calculate.inputs import (
    RawrRequestDTO,
    RawrSeasonInputDTO,
)
from rawr_analytics.metrics.rawr.progress import RawrProgressSink, emit_rawr_progress
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class RawrPlayerSeasonRecord:
    season: Season
    player: PlayerSummary
    minutes: PlayerMinutes
    games: int
    coefficient: float


def build_player_season_records(
    request: RawrRequestDTO,
    *,
    progress_sink: RawrProgressSink | None = None,
) -> list[RawrPlayerSeasonRecord]:
    season_inputs = sorted(request.season_inputs, key=lambda item: item.season.id)
    total_seasons = len(season_inputs)

    if total_seasons == 0:
        emit_rawr_progress(
            progress_sink,
            phase="model",
            current=1,
            total=1,
            detail="No complete season inputs produced RAWR observations.",
        )
        return []

    records: list[RawrPlayerSeasonRecord] = []
    for season_index, season_input in enumerate(season_inputs, start=1):
        records.extend(
            _build_season_records(
                season_input,
                request=request,
                progress_sink=progress_sink,
                season_index=season_index,
                total_seasons=total_seasons,
            )
        )

    records.sort(
        key=lambda record: (
            record.season.id,
            record.coefficient,
            record.player.player_name,
        ),
        reverse=True,
    )

    emit_rawr_progress(
        progress_sink,
        phase="model",
        current=total_seasons,
        total=total_seasons,
        detail=f"Built {len(records)} RAWR player-season rows.",
    )
    return records


def _build_season_records(
    season_input: RawrSeasonInputDTO,
    *,
    request: RawrRequestDTO,
    progress_sink: RawrProgressSink | None,
    season_index: int,
    total_seasons: int,
) -> list[RawrPlayerSeasonRecord]:
    player_contexts = season_input.players_by_id
    games_by_player_id = count_player_season_games(season_input.observations)
    eligible_player_ids = sorted(
        player_id
        for player_id, games in games_by_player_id.items()
        if games >= request.eligibility.min_games
    )
    if not eligible_player_ids:
        emit_rawr_progress(
            progress_sink,
            phase="model",
            current=season_index,
            total=total_seasons,
            detail=(
                f"{season_input.season.year_string_nba_api}: no players met the "
                f"minimum games threshold."
            ),
        )
        return []

    season_label = season_input.season.year_string_nba_api

    def publish_model_progress(current: int, total: int, detail: str | None) -> None:
        suffix = "" if detail is None else f": {detail}"
        emit_rawr_progress(
            progress_sink,
            phase="model",
            current=season_index - 1,
            total=total_seasons,
            detail=f"{season_label}{suffix}",
        )

    coefficients_by_player_id = fit_player_rawr(
        season_input.observations,
        player_ids=eligible_player_ids,
        season=season_input.season,
        ridge_alpha=request.ridge_alpha,
        shrinkage_mode=request.shrinkage_mode,
        shrinkage_strength=request.shrinkage_strength,
        shrinkage_minute_scale=request.shrinkage_minute_scale,
        progress=publish_model_progress,
    )

    records: list[RawrPlayerSeasonRecord] = []
    for player_id, coefficient in coefficients_by_player_id.items():
        player = player_contexts[player_id]
        if not player.passes_minute_filters(request.filters):
            continue
        records.append(
            RawrPlayerSeasonRecord(
                season=season_input.season,
                player=player.player,
                minutes=player.minutes,
                games=games_by_player_id[player_id],
                coefficient=coefficient,
            )
        )

    emit_rawr_progress(
        progress_sink,
        phase="model",
        current=season_index,
        total=total_seasons,
        detail=f"{season_label}: finished regression and filtering.",
    )
    return records
