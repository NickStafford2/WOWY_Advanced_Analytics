# Data Refactor Handoff

This is the handoff doc for the `src/rawr_analytics/data` rewrite.

If this file and older notes disagree, follow this file.

## Current Read On The Code

The package is not a single data layer. It is three different things:

1. normalized game cache persistence
2. derived metric snapshot persistence
3. metric refresh orchestration

The first two are now separated better than before. The remaining weak spot is the partly normalized scope metadata inside `data/metric_store`.

## Important Current Facts

### Physical DB split

The first slice is complete.

Current paths:

- `src/rawr_analytics/data/_paths.py`
- `data/app/normalized_cache.sqlite3`
- `data/app/metric_store.sqlite3`

The old shared file path `data/app/player_metrics.sqlite3` is now treated as legacy cleanup only.

### Cache schema location

Normalized cache tables live under:

- `src/rawr_analytics/data/game_cache/schema.py`
- `src/rawr_analytics/data/game_cache/repository.py`

The cache design is not perfect, but it is at least a recognizable bounded context.

### Metric DB location

Derived metric tables live under:

- `src/rawr_analytics/data/metric_store/schema.py`
- `src/rawr_analytics/data/metric_store/store.py`
- `src/rawr_analytics/data/metric_store/queries.py`

The rename is complete. The package name now matches the bounded context.

### Wrong-layer orchestration

Refresh orchestration now lives under:

- `src/rawr_analytics/services/metric_refresh/_refresh.py`
- `src/rawr_analytics/services/metric_refresh/__init__.py`

`data/metric_store` is now a persistence package again.

### Metric-store type boundary

The generic metric row contract has been removed.

Current state:

- `src/rawr_analytics/data/metric_store/models.py` no longer defines `PlayerSeasonMetricRow`
- RAWR validation and audit now use `RawrPlayerSeasonValueRow`
- WOWY validation and audit now use `WowyPlayerSeasonValueRow`
- the old generic query path in `src/rawr_analytics/data/metric_store/queries.py` was deleted

That slice is complete.

### Query-facing glue cleanup

This slice is complete.

- `src/rawr_analytics/data/metric_store_query.py` was deleted
- `src/rawr_analytics/data/metric_store_views.py` was deleted
- cache freshness checks now live in `src/rawr_analytics/metrics/metric_query/scope.py`
- scope snapshot assembly now lives in `src/rawr_analytics/metrics/metric_query/scope.py`
- span-chart shaping now lives in `src/rawr_analytics/metrics/metric_query/views.py`

### Metadata ownership cleanup

This slice is complete.

- `MetricStoreMetadata` no longer includes `label`
- `metric_store_metadata_v2` no longer has live code dependencies
- `label` is owned by `metric_scope_catalog`
- this removed a fresh-DB inconsistency where code queried and inserted a metadata `label` column that the schema did not define

### Snapshot parent table

This slice is now started but not finished.

Current state:

- `metric_snapshot` now exists and owns `snapshot_id` plus build metadata
- scope definition still lives separately in `metric_scope_catalog`, `metric_scope_team`, and `metric_scope_season`
- RAWR writes now create a snapshot row and store `snapshot_id` on `rawr_player_season_values`
- RAWR reads now join through `metric_snapshot`
- WOWY writes now create a snapshot row and store `snapshot_id` on `wowy_player_season_values`
- the main cached WOWY read path now joins through `metric_snapshot`
- full-span writes and reads now join through `metric_snapshot`
- WOWY value rows still physically keep the older `(metric_id, scope_key)` columns even though the main cached path reads through `snapshot_id`
- `MetricStoreMetadata` now includes optional `snapshot_id`
- old pre-snapshot rows are intentionally treated as stale and will rebuild

## Decisions Already Made

- rebuilding the DB from scratch is acceptable
- migration compatibility should not drive the design
- do not split into one DB file per metric right now
- do split normalized cache and metric store into separate DB files
- keep metric-specific value rows explicit
- move orchestration out of `data`

## Completed Slices

### Slice 1: physical DB split

- added `src/rawr_analytics/data/_paths.py`
- deleted `src/rawr_analytics/data/constants.py`
- wired `src/rawr_analytics/data/game_cache/*` to `NORMALIZED_CACHE_DB_PATH`
- wired the metric-store package to `METRIC_STORE_DB_PATH`
- removed legacy metric rename migration code from `src/rawr_analytics/data/metric_store/schema.py`
- updated rebuild cleanup in `src/rawr_analytics/services/rebuild.py` to delete both new DB files and the old mixed path
- updated validation and cache fingerprint code so they no longer depend on the mixed-layout DB

### Slice 2: package rename

- renamed `src/rawr_analytics/data/player_metrics_db/` to `src/rawr_analytics/data/metric_store/`
- updated imports to use `rawr_analytics.data.metric_store`
- kept the schema and row shapes unchanged
- did not preserve the old package name as compatibility glue

### Slice 3: service extraction

- created `src/rawr_analytics/services/metric_refresh/`
- moved metric refresh planning/build logic there
- deleted `src/rawr_analytics/data/_metric_store_refresh.py`
- deleted `src/rawr_analytics/services/metric_store.py`

### Slice 4: explicit metric row contracts

- deleted `PlayerSeasonMetricRow` from `src/rawr_analytics/data/metric_store/models.py`
- updated `src/rawr_analytics/data/metric_store/_validation.py` to validate RAWR and WOWY rows directly
- updated `src/rawr_analytics/data/metric_store/audit.py` and `src/rawr_analytics/data/db_validation.py` to track RAWR and WOWY rows explicitly
- deleted the unused generic metric row query path from `src/rawr_analytics/data/metric_store/queries.py`
- kept the SQLite schema unchanged

### Slice 5: delete old metric-store wrappers

- deleted `src/rawr_analytics/data/metric_store_rawr.py`
- deleted `src/rawr_analytics/data/metric_store_wowy.py`
- updated `src/rawr_analytics/metrics/metric_query/views.py` to read directly from `rawr_analytics.data.metric_store`
- kept the `metric_store` public API small instead of recreating wrapper glue

### Slice 6: move query-facing snapshot logic out of `data`

- deleted `src/rawr_analytics/data/metric_store_query.py`
- deleted `src/rawr_analytics/data/metric_store_views.py`
- moved cache freshness checks and scope snapshot assembly into `src/rawr_analytics/metrics/metric_query/scope.py`
- moved span-chart payload shaping into `src/rawr_analytics/metrics/metric_query/views.py`
- kept `src/rawr_analytics/data/metric_store/` persistence-only

### Slice 7: shrink metric-store metadata

- removed `label` from `src/rawr_analytics/data/metric_store/models.py::MetricStoreMetadata`
- updated metadata reads and writes to use only freshness fields plus row count
- updated audit and validation paths to stop expecting `label` on metadata
- left `label` owned by `metric_scope_catalog`

### Slice 8: replace scope team JSON with a real relation

- removed `available_team_ids_json` from `metric_scope_catalog`
- added `metric_scope_team` with primary key `(metric_id, scope_key, team_id)`
- updated metric-store writes to replace scope-team rows alongside catalog rows
- updated metric-store reads to hydrate `available_team_ids` from `metric_scope_team`
- updated audit and DB validation to check the new scope-team relation

### Slice 9: replace scope season JSON with a real relation

- removed `available_season_ids_json` from `metric_scope_catalog`
- added `metric_scope_season` with primary key `(metric_id, scope_key, season_id)`
- updated metric-store writes to replace scope-season rows alongside catalog rows
- updated metric-store reads to hydrate `available_season_ids` from `metric_scope_season`
- updated audit and DB validation to check the new scope-season relation
- kept the current `(metric_id, scope_key)` value-table layout in place

### Slice 10: add the first snapshot parent path

- added `metric_snapshot` with `snapshot_id`, `metric_id`, `scope_key`, build metadata, and row count
- added `snapshot_id` to `rawr_player_season_values`
- updated RAWR writes to insert `metric_snapshot` first and then write value rows with that `snapshot_id`
- updated RAWR reads to join through `metric_snapshot`
- updated metadata reads so both RAWR and WOWY use `metric_snapshot`
- updated audit and DB validation so RAWR build-state issues are attributed to `metric_snapshot`
- kept most WOWY value-table and full-span storage on the old `(metric_id, scope_key)` keys for now

### Slice 11: move WOWY build metadata onto snapshot

- added `snapshot_id` to `wowy_player_season_values`
- updated WOWY writes to insert `metric_snapshot` first and then write value rows with that `snapshot_id`
- updated `load_metric_store_metadata()` so it no longer falls back to `metric_store_metadata_v2`
- updated the main cached WOWY row loader to join through `metric_snapshot`
- updated audit and DB validation so WOWY build-state issues are also attributed to `metric_snapshot`
- kept scope definition separate from build state
- kept current WOWY/full-span table shapes to avoid widening the slice

### Slice 12: move full-span storage onto snapshot

- removed the last live code references to `metric_store_metadata_v2`
- changed `metric_full_span_series` to store rows by `snapshot_id`
- changed `metric_full_span_points` to store rows by `snapshot_id`
- updated metric-store writes and clears to delete full-span rows through `metric_snapshot`
- updated full-span reads to join through `metric_snapshot`
- updated audit to attribute full-span rows through `metric_snapshot`
- kept scope definition separate from build state
- did not widen the slice into a full WOWY value-table redesign

Verification already done:

- `poetry run python -m py_compile $(find src -name '*.py' -print)` passed
- `poetry run ruff check src/rawr_analytics/data/metric_store src/rawr_analytics/data/db_validation.py src/rawr_analytics/metrics/metric_query/views.py` passed
- `poetry run python -m py_compile src/rawr_analytics/data/metric_store/models.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/metric_store/_validation.py src/rawr_analytics/metrics/metric_query/scope.py src/rawr_analytics/metrics/metric_query/views.py` passed
- `poetry run ruff check src/rawr_analytics/data/metric_store/models.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/metric_store/_validation.py src/rawr_analytics/metrics/metric_query/scope.py src/rawr_analytics/metrics/metric_query/views.py` passed
- `poetry run python -m py_compile src/rawr_analytics/data/metric_store/schema.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/db_validation.py` passed
- `poetry run ruff check src/rawr_analytics/data/metric_store/schema.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/db_validation.py` passed
- `poetry run python -m py_compile src/rawr_analytics/data/metric_store/models.py src/rawr_analytics/data/metric_store/schema.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/rawr.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/db_validation.py` passed
- `poetry run ruff check src/rawr_analytics/data/metric_store/models.py src/rawr_analytics/data/metric_store/schema.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/rawr.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/db_validation.py` passed
- `poetry run python -m py_compile src/rawr_analytics/data/metric_store/schema.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/wowy.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/db_validation.py src/rawr_analytics/services/metric_refresh/_refresh.py src/rawr_analytics/metrics/metric_query/views.py` passed
- `poetry run ruff check src/rawr_analytics/data/metric_store/schema.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/wowy.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/db_validation.py src/rawr_analytics/services/metric_refresh/_refresh.py src/rawr_analytics/metrics/metric_query/views.py` passed
- `poetry run python -m py_compile src/rawr_analytics/data/metric_store/models.py src/rawr_analytics/data/metric_store/full_span.py src/rawr_analytics/data/metric_store/schema.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/db_validation.py src/rawr_analytics/metrics/metric_query/views.py` passed
- `poetry run ruff check src/rawr_analytics/data/metric_store/models.py src/rawr_analytics/data/metric_store/full_span.py src/rawr_analytics/data/metric_store/schema.py src/rawr_analytics/data/metric_store/store.py src/rawr_analytics/data/metric_store/queries.py src/rawr_analytics/data/metric_store/audit.py src/rawr_analytics/data/db_validation.py src/rawr_analytics/metrics/metric_query/views.py` passed

Local DB state when this handoff was written:

- no live `data/app/player_metrics.sqlite3` file was present
- only `.bak` files remained under `data/app/`
- later, the live derived DB was explicitly deleted and will be rebuilt from normalized cache

## Recommended Target Shape

Use these bounded contexts:

### `data/game_cache`

Owns:

- normalized game rows
- normalized game player rows
- cache load metadata
- cache fingerprints

Should not own:

- metric refresh planning
- derived metric snapshots

### `data/metric_store`

Owns:

- metric snapshot metadata
- metric scope metadata
- metric-specific persisted rows
- full-span derived tables for fast reads

Should not own:

- metric computation
- cache freshness planning policy

### `services/metric_refresh`

Owns:

- refresh planning
- deciding which scopes to rebuild
- invoking metric computation
- calling data-layer write functions

## What To Delete

## Delete now or during the first pass

- schema-preservation code that only exists for old layouts if it blocks clarity
- any remaining local `data/app/player_metrics.sqlite3` file if it reappears

## Delete after replacement exists

- the `metric_store_metadata_v2` table definition itself, now that no code reads or clears it
- schema-preservation helpers in `src/rawr_analytics/data/metric_store/schema.py` after the DB is rebuilt from scratch

## Delete from repo if tracked

- `src/rawr_analytics/data/__pycache__/`
- `src/rawr_analytics/data/game_cache/__pycache__/`
- `src/rawr_analytics/data/metric_store/__pycache__/`

## Suggested Execution Order

1. Rename `player_metrics_db` to `metric_store`.
2. Move refresh logic into `services/metric_refresh`.
3. Replace generic metric row APIs with explicit RAWR/WOWY APIs.
4. Delete the old wrappers and remaining glue code.
5. Rebuild the metric-store schema around scope and snapshot tables.

## Risks To Watch

### Risk 1: accidental new abstractions

Do not replace the current mess with a large generic repository framework.
Keep functions small and concrete.

### Risk 2: preserving convenience fields everywhere

It will be tempting to keep `team_filter`, `season_type`, and `label` on every value row.
Avoid that unless a query truly requires denormalization.

### Risk 3: generic metric contracts creeping back in

Do not reintroduce another “universal metric row” type.
RAWR and WOWY should stay explicit.

## Next Concrete Coding Slice

The best next slice is:

1. delete the `metric_store_metadata_v2` table definition and any remaining schema-preservation code around it
2. move one more small WOWY value-table path from `(metric_id, scope_key)` assumptions toward `snapshot_id`
3. prefer read/query or validation cleanup over a broad table redesign
4. keep scope definition tables separate from build-state tables
5. do not attempt the full WOWY value-table redesign or `metric_snapshot_season` in the same pass

That keeps the next step concrete and continues the snapshot redesign without turning it into a giant rewrite.

## Suggested Commit Message

`docs: refresh data-layer refactor status`
