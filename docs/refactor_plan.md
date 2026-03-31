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
- Cleaned up `src/rawr_analytics/data/player_metrics_db/store.py` to import the current public validation functions.
- Kept DB-facing season fields on `season_id` and aligned WOWY custom-query core rows with that naming.
- Rewrote the Phase 3 plan to prefer metric-specific query contracts and separate metric tables over a shared metric abstraction layer.
- Added `src/rawr_analytics/data/player_metrics_db/rawr.py` and `src/rawr_analytics/data/player_metrics_db/wowy.py` with metric-specific cached row models and repository loaders.
- Split cached season-value storage into `rawr_player_season_values` and `wowy_player_season_values` in `src/rawr_analytics/data/player_metrics_db/schema.py`.
- Rewired metric-store persistence, cached-row views, and metric-store audit code to use the new metric-specific tables.
- Added typed RAWR and WOWY custom-query result models and rewired `src/rawr_analytics/metrics/metric_query/views.py` to dispatch through them.
- Removed the remaining shared row-normalization path from `src/rawr_analytics/metrics/metric_query/views.py` by splitting RAWR and WOWY leaderboard, export, and player-season assembly into separate typed branches.

## Remaining

1. Split `src/rawr_analytics/data/metric_store.py` and `src/rawr_analytics/data/metric_store_views.py` by concern so refresh policy, computation, persistence, and snapshot loading are no longer coupled behind generic store helpers.
2. Push the remaining metric-store read/write contracts toward explicit RAWR-owned and WOWY-owned repository paths where behavior or row shape differs.
3. Remove stale deep imports inside ingest and metric internals once the new public surfaces are stable enough to consume directly.
4. Unify metric-store fingerprint logic so refresh-time and read-time freshness checks share one implementation.

## Verification

- `poetry run ruff check src scripts` passed.
- `poetry run python -m py_compile ...` passed for the changed service, web, and script files.
- `poetry run pyright src scripts` still fails on pre-existing import errors in `src/rawr_analytics/data/player_metrics_db/store.py`; those errors were not introduced by this Phase 1 work.
- `poetry run ruff check --fix src/rawr_analytics/metrics src/rawr_analytics/data/metric_store.py` passed after fixing import ordering in the Phase 2 files.
- `poetry run python -m py_compile src/rawr_analytics/metrics/rawr/dataset.py src/rawr_analytics/metrics/wowy/dataset.py src/rawr_analytics/data/metric_store.py src/rawr_analytics/metrics/metric_query/views.py src/rawr_analytics/metrics/rawr/cli.py src/rawr_analytics/metrics/wowy/cli.py src/rawr_analytics/metrics/rawr/tuning.py` passed.
- `poetry run pyright src/rawr_analytics/metrics/rawr/dataset.py src/rawr_analytics/metrics/wowy/dataset.py src/rawr_analytics/data/metric_store.py src/rawr_analytics/metrics/metric_query/views.py src/rawr_analytics/metrics/rawr/cli.py src/rawr_analytics/metrics/wowy/cli.py src/rawr_analytics/metrics/rawr/tuning.py` passed.
- `poetry run python -m py_compile` passed for the new metric-specific DB and metric-query files.
- `poetry run ruff check` passed for the touched Phase 3 files.
- `poetry run pyright` passed for the touched Phase 3 files.
- `poetry run python -m py_compile src/rawr_analytics/metrics/metric_query/views.py` passed after splitting RAWR and WOWY query assembly paths.
- `poetry run ruff check src/rawr_analytics/metrics/metric_query/views.py` passed after removing the shared row-normalization path.
- `poetry run pyright src/rawr_analytics/metrics/metric_query/views.py` passed after adding explicit cached-row narrowing for RAWR and WOWY.
- `poetry run python -m py_compile $(find src scripts -name '*.py' -print)` passed.
- `poetry run ruff check` currently fails in the pre-existing `tests/` tree; the new Phase 3 query-view file passes its own Ruff check.
- `poetry run pyright` currently fails broadly in the pre-existing `tests/` tree; the new Phase 3 query-view file passes its own Pyright check.

Tests were not run, per the current rebuild instructions.
