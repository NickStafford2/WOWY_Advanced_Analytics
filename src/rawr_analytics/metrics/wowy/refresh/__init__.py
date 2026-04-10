"""WOWY metric store refresh workflow.

Inputs are refresh scopes and calculated WOWY records. Outputs are metric store
rows ready to replace WOWY snapshots.
"""

from rawr_analytics.metrics.wowy.refresh.store_rows import build_wowy_metric_store_rows

__all__ = [
    "build_wowy_metric_store_rows",
]
