# Data Refactor Handoff

This is the handoff doc for the `src/rawr_analytics/data` rewrite.

If this file and older notes disagree, follow this file.

## Current Read On The Code

The package is not a single data layer. It is three different things:

1. normalized game cache persistence
2. derived metric snapshot persistence
3. metric refresh orchestration

Those concerns are currently interleaved.

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

- `src/rawr_analytics/data/player_metrics_db/schema.py`
- `src/rawr_analytics/data/player_metrics_db/store.py`
- `src/rawr_analytics/data/player_metrics_db/queries.py`

The package name is misleading. It is not just “player metrics DB.” It is the full derived metric snapshot store.

### Wrong-layer orchestration

`src/rawr_analytics/data/metric_store.py` currently performs service-layer work:

- refresh planning
- stale checks
- metric computation calls
- snapshot replacement decisions

This module should leave `data/` entirely.

### Weak type boundary

`PlayerSeasonMetricRow` in `src/rawr_analytics/data/player_metrics_db/models.py` is too generic.

Problems:

- it hides metric differences behind one row shape
- it uses `details: dict[str, Any] | None`
- it encourages generic query code instead of explicit contracts

This should be removed after the new query API is in place.

## Decisions Already Made

- rebuilding the DB from scratch is acceptable
- migration compatibility should not drive the design
- do not split into one DB file per metric right now
- do split normalized cache and metric store into separate DB files
- keep metric-specific value rows explicit
- move orchestration out of `data`

## Completed In The First Slice

- added `src/rawr_analytics/data/_paths.py`
- deleted `src/rawr_analytics/data/constants.py`
- wired `src/rawr_analytics/data/game_cache/*` to `NORMALIZED_CACHE_DB_PATH`
- wired `src/rawr_analytics/data/player_metrics_db/*` to `METRIC_STORE_DB_PATH`
- removed legacy metric rename migration code from `src/rawr_analytics/data/player_metrics_db/schema.py`
- updated rebuild cleanup in `src/rawr_analytics/services/rebuild.py` to delete both new DB files and the old mixed path
- updated validation and cache fingerprint code so they no longer depend on the mixed-layout DB

Verification already done:

- `poetry run python -m compileall src` passed

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

- `src/rawr_analytics/data/metric_store.py`
- `src/rawr_analytics/data/metric_store_rawr.py`
- `src/rawr_analytics/data/metric_store_wowy.py`
- `src/rawr_analytics/data/player_metrics_db/`

## Delete from repo if tracked

- `src/rawr_analytics/data/__pycache__/`
- `src/rawr_analytics/data/game_cache/__pycache__/`
- `src/rawr_analytics/data/player_metrics_db/__pycache__/`

## Suggested Execution Order

1. Rename `player_metrics_db` to `metric_store`.
2. Move refresh logic into `services/metric_refresh`.
3. Rebuild the metric-store schema around scope and snapshot tables.
4. Replace generic metric row APIs with explicit RAWR/WOWY APIs.
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

1. rename `src/rawr_analytics/data/player_metrics_db` to `src/rawr_analytics/data/metric_store`
2. update imports only
3. keep the schema and row shapes as-is for now
4. keep the public API small
5. do not move orchestration in the same pass

That gives naming clarity without mixing together rename work, service extraction, and schema redesign.

## Suggested Commit Message

`docs: add data-layer refactor plan and handoff`
