"""Owns derived analytics built from canonical inputs.

Examples:

- metric-specific input shaping
- metric computation
- metric-native derived records
- metric-specific CLI/report orchestration
"""

from typing import Literal

type MetricView = Literal[
    "player-seasons",
    "span-chart",
    "cached-leaderboard",
    "custom-query",
]

__all__ = ["MetricView"]
