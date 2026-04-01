# Data Refactor Handoff

This is the handoff doc for the `src/rawr_analytics/data` rewrite.

If this file and older notes disagree, follow this file.

## Current Read On The Code

The package is not a single data layer. It is three different things:

1. normalized game cache persistence
2. derived metric snapshot persistence
3. metric refresh orchestration

The first two are now separated better than before. The remaining weak spot is the generic metric row contract inside `data/metric_store`.

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

### Weak type boundary

`PlayerSeasonMetricRow` in `src/rawr_analytics/data/metric_store/models.py` is still too generic.

Problems:

- it hides metric differences behind one row shape
- it uses `details: dict[str, Any] | None`
- it encourages generic query code instead of explicit contracts

This is the next slice.

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

Verification already done:

- `poetry run python -m py_compile $(find src -name '*.py' -print)` passed

Local DB state when this handoff was written:

- no live `data/app/player_metrics.sqlite3` file was present
- only `.bak` files remained under `data/app/`

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

- `src/rawr_analytics/data/metric_store_rawr.py`
- `src/rawr_analytics/data/metric_store_wowy.py`

## Delete from repo if tracked

- `src/rawr_analytics/data/__pycache__/`
- `src/rawr_analytics/data/game_cache/__pycache__/`
- `src/rawr_analytics/data/metric_store/__pycache__/`

## Suggested Execution Order

1. Rename `player_metrics_db` to `metric_store`.
2. Move refresh logic into `services/metric_refresh`.
3. Replace generic metric row APIs with explicit RAWR/WOWY APIs.
4. Rebuild the metric-store schema around scope and snapshot tables.
5. Delete the old wrappers and remaining glue code.

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

1. replace `PlayerSeasonMetricRow` in `src/rawr_analytics/data/metric_store/models.py`
2. keep explicit RAWR and WOWY row contracts only
3. update `queries.py`, `audit.py`, and `db_validation.py` to use those explicit types
4. keep the current SQLite schema unless a tiny repair is required
5. do not start the snapshot-table schema rewrite in the same pass

That strengthens the type boundary without mixing together contract cleanup and schema redesign.

## Suggested Commit Message

`docs: refresh data-layer refactor status`
