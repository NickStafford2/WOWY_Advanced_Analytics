from __future__ import annotations

from typing import Any, Iterable

from rawr_analytics.data.player_metrics_db.models import PlayerSeasonMetricRow


def build_rawr_player_season_metric_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    records: Iterable[Any],
) -> list[PlayerSeasonMetricRow]:
    return [
        PlayerSeasonMetricRow(
            metric="rawr",
            metric_label="RAWR",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            season=record.season,
            player_id=record.player_id,
            player_name=record.player_name,
            value=record.coefficient,
            sample_size=record.games,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
            details={"games": record.games},
        )
        for record in records
    ]


def build_wowy_player_season_metric_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    metric: str,
    metric_label: str,
    records: Iterable[Any],
    values_by_player_season: dict[tuple[str, int], float] | None = None,
    include_raw_wowy_score: bool = False,
) -> list[PlayerSeasonMetricRow]:
    rows: list[PlayerSeasonMetricRow] = []
    for record in records:
        value = (
            values_by_player_season[(record.season, record.player_id)]
            if values_by_player_season is not None
            else record.wowy_score
        )
        details = {
            "games_with": record.games_with,
            "games_without": record.games_without,
            "avg_margin_with": record.avg_margin_with,
            "avg_margin_without": record.avg_margin_without,
        }
        if include_raw_wowy_score:
            details["raw_wowy_score"] = record.wowy_score
        rows.append(
            PlayerSeasonMetricRow(
                metric=metric,
                metric_label=metric_label,
                scope_key=scope_key,
                team_filter=team_filter,
                season_type=season_type,
                season=record.season,
                player_id=record.player_id,
                player_name=record.player_name,
                value=value,
                sample_size=record.games_with,
                secondary_sample_size=record.games_without,
                average_minutes=record.average_minutes,
                total_minutes=record.total_minutes,
                details=details,
            )
        )
    return rows
