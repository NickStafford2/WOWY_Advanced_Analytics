from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.player import PlayerSummary


@dataclass(frozen=True)
class MetricFullSpanSeries:
    player: PlayerSummary
    span_average_value: float
    season_count: int
    rank_order: int
    points_by_season: dict[str, float]


@dataclass(frozen=True)
class MetricStorePlayerSeasonValue:
    player_id: int
    player_name: str
    season_id: str
    value: float


def build_metric_full_span_series(
    *,
    season_ids: list[str],
    player_season_values: list[MetricStorePlayerSeasonValue],
    top_n: int | None = None,
) -> list[MetricFullSpanSeries]:
    totals: dict[int, float] = {}
    counts: dict[int, int] = {}
    names: dict[int, str] = {}
    season_values: dict[int, dict[str, float]] = {}

    for row in player_season_values:
        player_id = row.player_id
        totals[player_id] = totals.get(player_id, 0.0) + row.value
        counts[player_id] = counts.get(player_id, 0) + 1
        names[player_id] = row.player_name
        season_values.setdefault(player_id, {})[row.season_id] = row.value

    span_length = len(season_ids) or 1
    ranked_player_ids = sorted(
        totals,
        key=lambda player_id: (totals[player_id], names[player_id]),
        reverse=True,
    )
    if top_n is not None:
        ranked_player_ids = ranked_player_ids[:top_n]

    series: list[MetricFullSpanSeries] = []
    for rank_order, player_id in enumerate(ranked_player_ids, start=1):
        series.append(
            MetricFullSpanSeries(
                player=PlayerSummary(
                    player_id=player_id,
                    player_name=names[player_id],
                ),
                span_average_value=totals[player_id] / span_length,
                season_count=counts[player_id],
                rank_order=rank_order,
                points_by_season={
                    season_id: season_values[player_id][season_id]
                    for season_id in season_ids
                    if season_id in season_values[player_id]
                },
            )
        )
    return series


__all__ = [
    "MetricFullSpanSeries",
    "MetricStorePlayerSeasonValue",
    "build_metric_full_span_series",
]
