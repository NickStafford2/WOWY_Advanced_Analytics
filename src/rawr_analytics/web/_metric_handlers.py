from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from flask import Response


@dataclass(frozen=True)
class MetricWebHandlers:
    json_options_response: Callable[[], Response]
    json_player_seasons_response: Callable[[], Response]
    json_span_chart_response: Callable[[], Response]
    json_leaderboard_response: Callable[[bool], Response]
    csv_leaderboard_response: Callable[[bool], Response]
