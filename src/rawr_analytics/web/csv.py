import json
from typing import Any

from rawr_analytics.metrics.constants import Metric


def render_leaderboard_csv(
    metric: Metric,
    table_rows: list[dict[str, Any]],
) -> str:
    import csv
    from io import StringIO

    output = StringIO()
    writer = csv.writer(output)
    column_order = _build_csv_column_order(table_rows)
    writer.writerow([_csv_header_label(column, metric=metric) for column in column_order])
    for row in table_rows:
        writer.writerow([_format_csv_value(row.get(column)) for column in column_order])
    return output.getvalue()


def _format_csv_value(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, separators=(",", ":"))
    if isinstance(value, float):
        return f"{value:.6g}"
    if value is None:
        return "—"
    return str(value)


def _build_csv_column_order(table_rows: list[dict[str, Any]]) -> list[str]:
    preferred_order = [
        "rank",
        "player_id",
        "player_name",
        "span_average_value",
        "average_minutes",
        "total_minutes",
        "games",
        "games_with",
        "games_without",
        "avg_margin_with",
        "avg_margin_without",
        "season_count",
        "points",
    ]
    available_columns = {key for row in table_rows for key in row}
    ordered_columns = [column for column in preferred_order if column in available_columns]
    ordered_columns.extend(sorted(available_columns - set(ordered_columns)))
    return ordered_columns


def _csv_header_label(column: str, *, metric: Metric) -> str:
    return {
        "rank": "Rank",
        "player_id": "Player ID",
        "player_name": "Player",
        "span_average_value": _metric_column_label(metric),
        "average_minutes": "Avg Min",
        "total_minutes": "Tot Min",
        "games": "Games",
        "games_with": "With",
        "games_without": "Without",
        "avg_margin_with": "Avg With",
        "avg_margin_without": "Avg Without",
        "season_count": "Seasons",
        "points": "Points",
    }.get(column, column)


def _metric_column_label(metric: Metric) -> str:
    assert isinstance(metric, Metric)
    match metric:
        case Metric.RAWR:
            return "RAWR"
        case Metric.WOWY:
            return "WOWY"
        case Metric.WOWY_SHRUNK:
            return "WOWY Shrunk"
        case _:
            raise ValueError(f"Unknown metric: {metric}")
