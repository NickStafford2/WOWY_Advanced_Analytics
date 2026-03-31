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

## Remaining

1. Move RAWR and WOWY dataset shaping out of `src/rawr_analytics/data/rawr.py` and `src/rawr_analytics/data/wowy.py` into `metrics/`.
2. Replace dict-heavy metric query payloads with typed core contracts before JSON and CSV serialization.
3. Split `src/rawr_analytics/data/metric_store.py` responsibility by concern instead of leaving refresh policy, computation, persistence, and snapshot shaping coupled together.
4. Remove stale deep imports inside ingest and metric internals once the new public surfaces are stable enough to consume directly.
5. Unify metric-store fingerprint logic so refresh-time and read-time freshness checks share one implementation.

## Verification

- `poetry run ruff check src scripts` passed.
- `poetry run python -m py_compile ...` passed for the changed service, web, and script files.
- `poetry run pyright src scripts` still fails on pre-existing import errors in `src/rawr_analytics/data/player_metrics_db/store.py`; those errors were not introduced by this Phase 1 work.

Tests were not run, per the current rebuild instructions.
