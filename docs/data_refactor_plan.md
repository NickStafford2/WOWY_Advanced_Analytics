# Data Refactor Plan

This document is the concrete refactor plan for `src/rawr_analytics/data`.

## Goal

Make the data layer boring again.

The target shape is:

- one bounded context for normalized source cache
- one bounded context for derived metric snapshots
- orchestration moved out of `data`
- explicit typed contracts instead of generic metric rows
- no migration work preserved just to protect old SQLite layouts

The user is willing to rebuild the DB from scratch. That means this plan should prefer deletion over migration whenever the old structure is confusing.

## Current Problems

### 1. One DB file was serving two different systems

This was the first problem and it is now addressed.

The old layout pointed both cache storage and metric storage at the same SQLite file:

- normalized cache tables are created in `src/rawr_analytics/data/game_cache/schema.py`
- metric result tables are created in `src/rawr_analytics/data/player_metrics_db/schema.py`

The new split uses:

- `data/app/normalized_cache.sqlite3`
- `data/app/metric_store.sqlite3`

These are different concerns and should not share a physical DB just because both happen to use SQLite.

### 2. `data` owns orchestration that belongs in a service layer

`src/rawr_analytics/data/metric_store.py` currently:

- inspects cache state
- plans scopes
- computes fingerprints
- calls metric code
- decides whether refresh is stale
- writes snapshots

That is not a repository module. It is application/service logic.

### 3. The metric DB is only partly normalized

The current metric tables repeat scope metadata on every metric row:

- `team_filter`
- `season_type`
- `metric_id`
- `scope_key`

Those values describe the snapshot or scope. They should mostly live in scope/snapshot tables, not be copied across every player-season row.

### 4. The public API is wider than the real design

There are several wrapper modules that mostly restate the same logic:

- `src/rawr_analytics/data/metric_store_rawr.py`
- `src/rawr_analytics/data/metric_store_wowy.py`
- parts of `src/rawr_analytics/data/player_metrics_db/__init__.py`

This makes the package look more complicated than it is.

Status:

- `src/rawr_analytics/data/metric_store_rawr.py` was deleted
- `src/rawr_analytics/data/metric_store_wowy.py` was deleted
- `src/rawr_analytics/data/metric_store_query.py` was deleted
- `src/rawr_analytics/data/metric_store_views.py` was deleted
- cache freshness, scope snapshot assembly, and span-chart shaping now live in `src/rawr_analytics/metrics/metric_query/`

### 5. Generic row contracts are weakening the type boundary

`PlayerSeasonMetricRow` in `src/rawr_analytics/data/player_metrics_db/models.py` uses a generic shape plus `details: dict[str, Any] | None`.

That is the opposite of the repo direction. RAWR and WOWY already have different native data. The data layer should expose explicit row types for each.

Status:

- completed
- `PlayerSeasonMetricRow` has been deleted from `src/rawr_analytics/data/metric_store/models.py`
- validation, audit, and DB validation now use explicit RAWR and WOWY row contracts

### 6. Metadata and scope catalog still overlap

The current metric-store metadata is still only partly normalized.

Current state:

- `label` now lives only on `metric_scope_catalog`
- `metric_snapshot` now exists as the parent build-state table
- RAWR now reads and writes through `snapshot_id`
- WOWY build metadata now also lives on `metric_snapshot`
- one cached WOWY write/read path now uses `snapshot_id`
- full-span rows now also write and read through `snapshot_id`
- `metric_store_metadata_v2` no longer has live code dependencies
- scope-team membership now lives in `metric_scope_team`
- scope-season membership now lives in `metric_scope_season`

The remaining schema work should keep moving metric data off `(metric_id, scope_key)` value rows and onto `snapshot_id` without mixing scope definition back into build state.

## Target Architecture

## Package split

Target package layout:

```text
src/rawr_analytics/
  data/
    _db.py
    _paths.py
    game_cache/
      __init__.py
      schema.py
      repository.py
      rows.py
      fingerprints.py
      _validation.py
    metric_store/
      __init__.py
      schema.py
      scope.py
      metadata.py
      rawr_rows.py
      wowy_rows.py
      query.py
      write.py
      full_span.py
      _validation.py
  services/
    metric_refresh/
      __init__.py
      plan.py
      refresh.py
      scope.py
```

Rules:

- `data/game_cache` owns only normalized-cache persistence
- `data/metric_store` owns only derived metric persistence
- `services/metric_refresh` owns refresh planning and rebuild logic
- `metrics` computes metric-native records and does not write SQLite directly

## Physical DB split

Use two DB paths:

- `data/app/normalized_cache.sqlite3`
- `data/app/metric_store.sqlite3`

Do not create one DB file per metric unless a real operational need appears later. RAWR and WOWY share snapshot scope, rebuild lifecycle, and query surfaces. They belong in the same derived-metric DB.

## Metric-store schema target

Use snapshot-centered tables.

Suggested minimum schema:

### `metric_scope`

- `scope_key TEXT PRIMARY KEY`
- `season_type TEXT NOT NULL`
- `label TEXT NOT NULL`

### `metric_scope_team`

- `scope_key TEXT NOT NULL`
- `team_id INTEGER NOT NULL`
- primary key `(scope_key, team_id)`

For all-teams scope, store zero rows rather than an `"all-teams"` pseudo-team row.

### `metric_snapshot`

- `snapshot_id INTEGER PRIMARY KEY`
- `metric_id TEXT NOT NULL`
- `scope_key TEXT NOT NULL`
- `build_version TEXT NOT NULL`
- `source_fingerprint TEXT NOT NULL`
- `updated_at TEXT NOT NULL`
- `row_count INTEGER NOT NULL`
- unique key `(metric_id, scope_key)`

### `metric_snapshot_season`

- `snapshot_id INTEGER NOT NULL`
- `season_id TEXT NOT NULL`
- primary key `(snapshot_id, season_id)`

### `rawr_player_season_value`

- `snapshot_id INTEGER NOT NULL`
- `season_id TEXT NOT NULL`
- `player_id INTEGER NOT NULL`
- `player_name TEXT NOT NULL`
- `coefficient REAL NOT NULL`
- `games INTEGER NOT NULL`
- `average_minutes REAL`
- `total_minutes REAL`
- primary key `(snapshot_id, season_id, player_id)`

### `wowy_player_season_value`

- `snapshot_id INTEGER NOT NULL`
- `metric_id TEXT NOT NULL`
- `season_id TEXT NOT NULL`
- `player_id INTEGER NOT NULL`
- `player_name TEXT NOT NULL`
- `value REAL NOT NULL`
- `games_with INTEGER NOT NULL`
- `games_without INTEGER NOT NULL`
- `avg_margin_with REAL NOT NULL`
- `avg_margin_without REAL NOT NULL`
- `average_minutes REAL`
- `total_minutes REAL`
- `raw_wowy_score REAL`
- primary key `(snapshot_id, season_id, player_id)`

### `metric_full_span_series`

- `snapshot_id INTEGER NOT NULL`
- `player_id INTEGER NOT NULL`
- `player_name TEXT NOT NULL`
- `span_average_value REAL NOT NULL`
- `season_count INTEGER NOT NULL`
- `rank_order INTEGER NOT NULL`
- primary key `(snapshot_id, player_id)`

### `metric_full_span_point`

- `snapshot_id INTEGER NOT NULL`
- `player_id INTEGER NOT NULL`
- `season_id TEXT NOT NULL`
- `value REAL NOT NULL`
- primary key `(snapshot_id, player_id, season_id)`

Notes:

- `scope_key` can remain a string if that is the simplest stable contract.
- `team_filter` should not be stored redundantly on every value row.
- if all query paths already know the metric, `metric_id` can be omitted from metric-specific value tables.
- the current codebase is in a transitional state where metadata, RAWR rows, WOWY cached rows, and full-span rows are snapshot-backed, but WOWY value rows still physically keep the older `(metric_id, scope_key)` columns

## Refactor Phases

## Phase 1: Split physical DB ownership

Objective:

- stop sharing one SQLite file between cache and metrics

Work:

- replace `DB_PATH` with `NORMALIZED_CACHE_DB_PATH` and `METRIC_STORE_DB_PATH`
- update `game_cache` to use only the cache DB path
- update metric-store modules to use only the metric DB path
- delete any compatibility code that exists only because the old tables shared one file

Success condition:

- rebuilding cache does not touch metric tables
- rebuilding metric snapshots does not touch normalized cache tables

Status:

- completed

Notes:

- `src/rawr_analytics/data/_paths.py` now defines the explicit DB paths
- `game_cache/*` reads and writes only `NORMALIZED_CACHE_DB_PATH`
- `metric_store/*` reads and writes only `METRIC_STORE_DB_PATH`
- `src/rawr_analytics/data/constants.py` was deleted
- `src/rawr_analytics/data/metric_store/schema.py` no longer preserves legacy metric rename migration logic
- validation and cache fingerprint code were updated so they no longer depend on the old mixed-layout DB assumption

## Phase 2: Rename `player_metrics_db` to `metric_store`

Objective:

- align naming with what the package really does

Work:

- rename `src/rawr_analytics/data/player_metrics_db` to `src/rawr_analytics/data/metric_store`
- keep the new package public API very small
- avoid exposing internal helper modules from `__init__.py`

Success condition:

- the package name describes the bounded context instead of the row shape

Keep this slice small:

- rename the package directory and imports only
- do not redesign the schema in the same pass
- keep the public API narrow
- delete wrapper modules only if the replacement is already clear and direct

Status:

- completed

Notes:

- `src/rawr_analytics/data/player_metrics_db/` was renamed to `src/rawr_analytics/data/metric_store/`
- imports now target `rawr_analytics.data.metric_store`
- the schema and persisted row shapes were intentionally left unchanged in this slice
- the old package name was removed instead of preserved behind compatibility glue

## Phase 3: Move refresh logic out of `data`

Objective:

- make repository code passive

Work:

- move planning and refresh logic from `src/rawr_analytics/data/metric_store.py`
- create `src/rawr_analytics/services/metric_refresh/`
- keep only read/write/query functions inside `data/metric_store`

Success condition:

- `data` no longer imports `rawr_analytics.metrics.*`

Status:

- completed

Notes:

- refresh orchestration now lives under `src/rawr_analytics/services/metric_refresh/`
- `src/rawr_analytics/data/` no longer owns refresh planning or metric computation calls
- the old `src/rawr_analytics/data/_metric_store_refresh.py` and `src/rawr_analytics/services/metric_store.py` modules were deleted
- persistence still lives under `src/rawr_analytics/data/metric_store/`

## Phase 4: Rebuild metric-store schema around snapshots

Objective:

- reduce repeated metadata and make the schema easier to reason about

Work:

- create `metric_scope`, `metric_scope_team`, `metric_snapshot`, and `metric_snapshot_season`
- rewrite write paths to replace one snapshot atomically
- rewrite queries to load metadata from snapshot/scope tables
- keep value tables metric-specific

Success condition:

- repeated scope metadata is removed from most value rows
- reads join through snapshot identity instead of copying metadata everywhere

Recommended first slice inside this phase:

- move build metadata to `metric_snapshot`
- thread one write/read path through `snapshot_id`
- keep the larger value-table rewrite incremental instead of doing it all at once

## Phase 5: Delete generic row contracts

Objective:

- strengthen data contracts

Work:

- delete `PlayerSeasonMetricRow`
- delete `load_metric_rows`
- expose explicit typed query functions for RAWR and WOWY only

Success condition:

- no `dict[str, Any]` details payload is needed for metric rows

Recommended first slice inside this phase:

- replace `PlayerSeasonMetricRow` with explicit RAWR and WOWY contracts
- update `queries.py`, `audit.py`, and `db_validation.py` to use those explicit types
- do not redesign the SQLite schema in the same pass

## Phase 6: Collapse wrapper modules

Objective:

- reduce surface area

Work:

- merge `metric_store_rawr.py` and `metric_store_wowy.py` into one query module if the remaining differences are trivial
- otherwise keep metric-specific query files under `data/metric_store/` with no extra wrapper layer

Success condition:

- one query path per use case
- no modules that only rename another function

## Deletion List

## Safe to delete immediately

These are good candidates because the user is willing to rebuild the DB:

- `data/app/player_metrics.sqlite3`
- any old derived-metric SQLite file that exists only for the current mixed layout
- legacy schema migration logic in `src/rawr_analytics/data/metric_store/schema.py`
- legacy metric rename migration logic in `src/rawr_analytics/data/metric_store/schema.py`

Specific code to remove during the refactor:

- `_migrate_legacy_metric_names()`
- schema compatibility branches that exist only to preserve older metric table names

## Explicit Non-Goals

- do not preserve backward compatibility for old DB files
- do not build a generic metric abstraction for future unknown metrics
- do not split into one DB file per metric right now
- do not preserve wrapper modules that add no behavior

## Current Recommended Slice

If continuing this refactor in code, start here:

1. delete `metric_store_metadata_v2` once no code depends on it
2. move another small WOWY or full-span path from `(metric_id, scope_key)` to `snapshot_id`
3. keep scope definition in scope tables and build state in snapshot rows
4. keep the current metric-specific value tables for now if that keeps the slice small
5. do not try to finish the whole snapshot redesign in one pass

That slice keeps the snapshot redesign moving without mixing it with a full value-table rewrite.
