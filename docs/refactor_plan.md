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
- Split cached leaderboard and player-season snapshot loading into `src/rawr_analytics/data/metric_store_rawr.py` and `src/rawr_analytics/data/metric_store_wowy.py`, leaving `src/rawr_analytics/data/metric_store_views.py` focused on the shared span snapshot path.
- Removed redundant `metric` and `metric_label` fields from the new metric-specific cached snapshot contracts; the query layer now derives that metadata from the metric it already knows.
- Moved the outer metric-store refresh loop into `src/rawr_analytics/services/metric_store.py` and changed `src/rawr_analytics/data/metric_store.py` to expose refresh planning and per-scope refresh helpers instead of owning the whole orchestration flow.
- Added `src/rawr_analytics/data/player_metrics_db/full_span.py` and moved full-span series/point row derivation there, keeping the DB-facing contract on `season_id`.
- Replaced the generic metric-store scope writer with explicit `replace_rawr_scope_snapshot()` and `replace_wowy_scope_snapshot()` repository functions in `src/rawr_analytics/data/player_metrics_db/store.py`.
- Simplified `src/rawr_analytics/data/metric_store.py` so per-scope refresh now builds metric-specific repository rows directly and dispatches to explicit RAWR or WOWY snapshot writers.
- Deleted `src/rawr_analytics/data/player_metrics_db/builders.py`; the remaining shared metric-store helper is the identical full-span series/point aggregation path in `src/rawr_analytics/data/player_metrics_db/full_span.py`.

## Remaining

1. Push the remaining metric-store read contracts toward explicit RAWR-owned and WOWY-owned repository paths where behavior or row shape differs.
2. Remove stale deep imports inside ingest and metric internals once the new public surfaces are stable enough to consume directly.
3. Unify metric-store fingerprint logic so refresh-time and read-time freshness checks share one implementation.


Tests were not run, per the current rebuild instructions.
