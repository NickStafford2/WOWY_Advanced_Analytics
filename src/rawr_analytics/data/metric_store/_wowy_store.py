from __future__ import annotations

from datetime import UTC, datetime

from rawr_analytics.data.metric_store._catalog import (
    MetricScopeCatalog,
    build_metric_scope_catalog_row,
)
from rawr_analytics.data.metric_store._replace import replace_metric_scope_snapshot
from rawr_analytics.data.metric_store._sql_writes import insert_wowy_rows
from rawr_analytics.data.metric_store._validation import validate_wowy_rows
from rawr_analytics.data.metric_store.full_span import build_wowy_full_span_rows
from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow


def replace_wowy_scope_snapshot(
    *,
    metric_id: str,
    scope_key: str,
    catalog: MetricScopeCatalog,
    build_version: str,
    source_fingerprint: str,
    rows: list[WowyPlayerSeasonValueRow],
) -> None:
    updated_at = datetime.now(UTC).isoformat()
    validate_wowy_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    series_rows, point_rows = build_wowy_full_span_rows(
        metric_id=metric_id,
        rows=rows,
        scope_key=scope_key,
        season_ids=catalog.availability.season_ids,
    )
    replace_metric_scope_snapshot(
        metric_id=metric_id,
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        catalog_row=build_metric_scope_catalog_row(
            metric_id=metric_id,
            scope_key=scope_key,
            catalog=catalog,
            updated_at=updated_at,
        ),
        series_rows=series_rows,
        point_rows=point_rows,
        insert_rows=lambda connection, snapshot_id: insert_wowy_rows(
            connection,
            rows,
            snapshot_id,
        ),
        row_count=len(rows),
    )
