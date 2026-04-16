from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.rawr.calculate.records import RawrPlayerSeasonRecord
from rawr_analytics.metrics.wowy.calculate.records import WowyPlayerSeasonRecord
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class AggregatedPlayerValue:
    player_id: int
    player_name: str
    value: float
    season_count: int


def aggregate_wowy_training_records(
    records: list[WowyPlayerSeasonRecord],
    aggregation: str,
) -> dict[int, AggregatedPlayerValue]:
    grouped: dict[int, list[WowyPlayerSeasonRecord]] = {}
    for record in records:
        grouped.setdefault(record.player.player_id, []).append(record)

    return {
        player_id: AggregatedPlayerValue(
            player_id=player_id,
            player_name=player_records[0].player.player_name,
            value=aggregate_values(
                [
                    record.result.value
                    for record in player_records
                    if record.result.value is not None
                ],
                [record.season for record in player_records if record.result.value is not None],
                aggregation,
            ),
            season_count=len(player_records),
        )
        for player_id, player_records in grouped.items()
    }


def aggregate_rawr_training_records(
    records: list[RawrPlayerSeasonRecord],
    aggregation: str,
) -> dict[int, AggregatedPlayerValue]:
    grouped: dict[int, list[RawrPlayerSeasonRecord]] = {}
    for record in records:
        grouped.setdefault(record.player.player_id, []).append(record)

    return {
        player_id: AggregatedPlayerValue(
            player_id=player_id,
            player_name=player_records[0].player.player_name,
            value=aggregate_values(
                [record.coefficient for record in player_records],
                [record.season for record in player_records],
                aggregation,
            ),
            season_count=len(player_records),
        )
        for player_id, player_records in grouped.items()
    }


def aggregate_values(
    values: list[float],
    seasons: list[Season],
    aggregation: str,
) -> float:
    if aggregation == "mean":
        return sum(values) / len(values)
    if aggregation == "max":
        return max(values)
    if aggregation == "latest":
        latest_index = max(
            range(len(seasons)),
            key=lambda index: (
                seasons[index].start_year,
                seasons[index].season_type.value,
            ),
        )
        return values[latest_index]
    raise ValueError(f"Unsupported aggregation: {aggregation}")


def build_holdout_targets(
    records: list[WowyPlayerSeasonRecord],
) -> dict[int, AggregatedPlayerValue]:
    targets: dict[int, AggregatedPlayerValue] = {}
    for record in records:
        if record.result.value is None:
            continue
        targets[record.player.player_id] = AggregatedPlayerValue(
            player_id=record.player.player_id,
            player_name=record.player.player_name,
            value=record.result.value,
            season_count=1,
        )
    return targets


__all__ = [
    "AggregatedPlayerValue",
    "aggregate_rawr_training_records",
    "aggregate_values",
    "aggregate_wowy_training_records",
    "build_holdout_targets",
]
