from __future__ import annotations

from rawr_analytics.data.metric_store.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
)
from rawr_analytics.data.metric_store.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow


def build_rawr_full_span_rows(
    *,
    rows: list[RawrPlayerSeasonValueRow],
    scope_key: str,
    season_ids: list[str],
) -> tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]:
    return _build_metric_full_span_rows(
        metric_id="rawr",
        scope_key=scope_key,
        season_ids=season_ids,
        player_season_values=[
            (row.player_id, row.player_name, row.season_id, row.coefficient) for row in rows
        ],
    )


def build_wowy_full_span_rows(
    *,
    metric_id: str,
    rows: list[WowyPlayerSeasonValueRow],
    scope_key: str,
    season_ids: list[str],
) -> tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]:
    return _build_metric_full_span_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        season_ids=season_ids,
        player_season_values=[
            (row.player_id, row.player_name, row.season_id, row.value) for row in rows
        ],
    )


def _build_metric_full_span_rows(
    *,
    metric_id: str,
    scope_key: str,
    season_ids: list[str],
    player_season_values: list[tuple[int, str, str, float]],
) -> tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]:
    totals: dict[int, float] = {}
    counts: dict[int, int] = {}
    names: dict[int, str] = {}
    season_values: dict[int, dict[str, float]] = {}

    for player_id, player_name, season_id, value in player_season_values:
        totals[player_id] = totals.get(player_id, 0.0) + value
        counts[player_id] = counts.get(player_id, 0) + 1
        names[player_id] = player_name
        season_values.setdefault(player_id, {})[season_id] = value

    span_length = len(season_ids) or 1
    ranked_player_ids = sorted(
        totals,
        key=lambda player_id: (totals[player_id], names[player_id]),
        reverse=True,
    )

    series_rows: list[MetricFullSpanSeriesRow] = []
    point_rows: list[MetricFullSpanPointRow] = []
    for rank_order, player_id in enumerate(ranked_player_ids, start=1):
        series_rows.append(
            MetricFullSpanSeriesRow(
                snapshot_id=None,
                metric_id=metric_id,
                scope_key=scope_key,
                player_id=player_id,
                player_name=names[player_id],
                span_average_value=totals[player_id] / span_length,
                season_count=counts[player_id],
                rank_order=rank_order,
            )
        )
        for season_id in season_ids:
            value = season_values[player_id].get(season_id)
            if value is None:
                continue
            point_rows.append(
                MetricFullSpanPointRow(
                    snapshot_id=None,
                    metric_id=metric_id,
                    scope_key=scope_key,
                    player_id=player_id,
                    season_id=season_id,
                    value=value,
                )
            )
    return series_rows, point_rows
