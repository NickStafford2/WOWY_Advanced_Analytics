## Completed

Phase 1 is now complete.

- Added `src/rawr_analytics/services/` as the public application boundary.
- Added `services/ingest.py`, `services/metric_store.py`, `services/metric_query.py`, and `services/rebuild.py`.
- Added typed service request/result models for ingest refresh, metric-store refresh, metric-query execution, and full rebuild orchestration.
- Moved `scripts/cache_season_data.py` to `services.ingest`.
- Replaced the stale subprocess-based rebuild flow in `scripts/rebuild_player_metrics_db.py` with a direct call to `services.rebuild.rebuild_player_metrics_db`.
- Moved Flask metric routes in `src/rawr_analytics/web/app.py` to `services.metric_query`.
- Moved `src/rawr_analytics/web/cli.py` and `src/rawr_analytics/web/refresh_cli.py` to `services.metric_store`.
- Kept long-running rebuild progress visible by passing ingest, metric-refresh, and validation progress callbacks through the service boundary.

Phase 2 is now complete.

- Added `src/rawr_analytics/metrics/rawr/dataset.py` and moved RAWR season-input loading, completeness-aware scope filtering, observation derivation, and custom-query row shaping there.
- Added `src/rawr_analytics/metrics/wowy/dataset.py` and moved WOWY season-input loading, game derivation, player-context building, and custom-query row shaping there.
- Updated `src/rawr_analytics/data/metric_store.py`, `src/rawr_analytics/metrics/metric_query/views.py`, `src/rawr_analytics/metrics/rawr/cli.py`, `src/rawr_analytics/metrics/wowy/cli.py`, and `src/rawr_analytics/metrics/rawr/tuning.py` to use the new public metric entrypoints.
- Exported the Phase 2 metric entrypoints from `src/rawr_analytics/metrics/rawr/__init__.py` and `src/rawr_analytics/metrics/wowy/__init__.py`.
- Deleted `src/rawr_analytics/data/rawr.py` and `src/rawr_analytics/data/wowy.py` after their metric-specific shaping responsibilities moved into `metrics/`.

## Remaining

1. Replace dict-heavy metric query payloads with typed core contracts before JSON and CSV serialization.
2. Split `src/rawr_analytics/data/metric_store.py` responsibility by concern instead of leaving refresh policy, computation, persistence, and snapshot shaping coupled together.
3. Remove stale deep imports inside ingest and metric internals once the new public surfaces are stable enough to consume directly.
4. Unify metric-store fingerprint logic so refresh-time and read-time freshness checks share one implementation.

## Verification

- `poetry run ruff check src scripts` passed.
- `poetry run python -m py_compile ...` passed for the changed service, web, and script files.
- `poetry run pyright src scripts` still fails on pre-existing import errors in `src/rawr_analytics/data/player_metrics_db/store.py`; those errors were not introduced by this Phase 1 work.
- `poetry run ruff check --fix src/rawr_analytics/metrics src/rawr_analytics/data/metric_store.py` passed after fixing import ordering in the Phase 2 files.
- `poetry run python -m py_compile src/rawr_analytics/metrics/rawr/dataset.py src/rawr_analytics/metrics/wowy/dataset.py src/rawr_analytics/data/metric_store.py src/rawr_analytics/metrics/metric_query/views.py src/rawr_analytics/metrics/rawr/cli.py src/rawr_analytics/metrics/wowy/cli.py src/rawr_analytics/metrics/rawr/tuning.py` passed.
- `poetry run pyright src/rawr_analytics/metrics/rawr/dataset.py src/rawr_analytics/metrics/wowy/dataset.py src/rawr_analytics/data/metric_store.py src/rawr_analytics/metrics/metric_query/views.py src/rawr_analytics/metrics/rawr/cli.py src/rawr_analytics/metrics/wowy/cli.py src/rawr_analytics/metrics/rawr/tuning.py` passed.
- `poetry run pyright src scripts` still fails only on the same pre-existing import errors in `src/rawr_analytics/data/player_metrics_db/store.py`.

Tests were not run, per the current rebuild instructions.
