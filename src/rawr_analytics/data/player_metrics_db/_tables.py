from __future__ import annotations


def metric_values_table(metric_id: str) -> str:
    if metric_id == "rawr":
        return "rawr_player_season_values"
    if metric_id in {"wowy", "wowy-shrunk"}:
        return "wowy_player_season_values"
    raise ValueError(f"Unknown metric table for {metric_id!r}")
