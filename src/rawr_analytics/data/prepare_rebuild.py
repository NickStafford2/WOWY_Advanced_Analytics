from __future__ import annotations

from datetime import UTC, datetime

from rawr_analytics.data._paths import (
    LEGACY_MIXED_DATA_DB_PATH,
    METRIC_STORE_DB_PATH,
    NORMALIZED_CACHE_DB_PATH,
)


def prepare_rebuild_storage(*, keep_existing_db: bool) -> bool:
    deleted_existing_db = False
    if not keep_existing_db:
        backup_suffix = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        for db_path in (
            NORMALIZED_CACHE_DB_PATH,
            METRIC_STORE_DB_PATH,
            LEGACY_MIXED_DATA_DB_PATH,
        ):
            if db_path.exists():
                backup_path = db_path.with_suffix(f"{db_path.suffix}.{backup_suffix}.bak")
                db_path.rename(backup_path)
                deleted_existing_db = True
    NORMALIZED_CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return deleted_existing_db


__all__ = ["prepare_rebuild_storage"]
