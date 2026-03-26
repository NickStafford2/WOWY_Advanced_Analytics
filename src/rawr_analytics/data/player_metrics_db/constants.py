from __future__ import annotations

from pathlib import Path

DEFAULT_PLAYER_METRICS_DB_PATH = Path("data/app/player_metrics.sqlite3")

LEGACY_METRIC_RENAMES = {
    "shrinkage_wowy": ("wowy_shrunk", "WOWY Shrunk"),
}
