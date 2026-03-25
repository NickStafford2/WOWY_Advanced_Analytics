# Architecture

This document is the working architectural guide for the repository.

It has two jobs:

1. describe the intended long-term shape of the codebase
2. tell the next Codex instance what to refactor next

Keep this file short. After each major refactor, update:

- the "Current Status" section
- the "Next Refactors" list

Do not turn this into a changelog.

This document is guidance, not a script.

- do not mechanically delete code just because this file says a boundary is wrong
- first determine whether the behavior is still useful
- if it is useful but misplaced, move it or narrow it
- if it is redundant or low-value, delete it
- prefer the smallest change that improves ownership and dependency direction
- during this refactor, you are free to avoid making shims and compatibility code. Tests are not as important as clarity and speed.

## Goal

Make the codebase easy to reason about by making ownership and dependency direction explicit.

The system is a layered pipeline:

1. source payloads are fetched and cached
2. payloads are parsed and normalized into canonical NBA records
3. canonical records are persisted and queried through the data layer
4. metrics are computed from canonical inputs
5. web and CLI surfaces consume metric outputs

Target dependency direction:

`source -> nba -> data -> metrics -> web/cli`

In practice, `nba` and `data` are both foundational, but the important rule is simpler:

- lower layers must not import higher layers

That means:

- `metrics` may depend on `nba` and `data`
- `web` and CLI may depend on `metrics`
- `nba` must not depend on `metrics`
- `data` must not depend on `metrics`

If a lower layer needs code from a higher layer, use judgment. Usually either:

- the code belongs in the lower layer and should move down, or
- the dependency is wrong and should be removed

## Layer Ownership

### `src/wowy/nba`

Owns canonical basketball-domain logic.

Examples:

- source payload parsing
- normalization into canonical game/player/team records
- canonical validation
- team identity and historical continuity rules

This layer should know nothing about WOWY, RAWR, or any other derived metric.

### `src/wowy/data`

Owns persistence and retrieval.

Examples:

- SQLite schema and repositories
- metric-store persistence
- cache metadata persistence
- loading canonical rows from the database
- writing derived metric results to the database

This layer should not contain metric math.

### `src/wowy/metrics`

This is the target home for what currently lives under `src/wowy/apps/`.

Owns derived analytics built from canonical inputs.

Examples:

- metric-specific input shaping
- metric computation
- metric-native derived records
- metric-specific CLI/report orchestration

Each metric package should prefer a small set of owner modules such as:

- `analysis.py`: metric math
- `inputs.py` or `derive.py`: canonical -> metric-native inputs
- `records.py`: metric-native derived records
- `service.py` or `cli.py`: CLI/report orchestration
- `formatting.py`: human-facing formatting only

### `src/wowy/web`

Owns presentation and endpoint wiring.

Examples:

- Flask route wiring
- payload shaping for frontend routes
- use of cached metric-store rows

This layer should depend on stable metric and data entrypoints, not low-level helpers from many modules.

### `src/wowy/shared`

Owns genuinely cross-cutting helpers that are neither metric-specific nor NBA-specific.

Keep this package small.

## Persistence Boundary

Derived metrics should not save themselves.

The intended direction is:

1. `metrics` computes derived outputs
2. `data` persists those outputs

Short term, two shapes are acceptable:

- `metrics` returns metric-native records and `data` maps them to DB rows
- `metrics` returns persistence-ready rows and `data` writes them

Long term, prefer the stricter shape:

- `metrics` owns metric-native records
- `data` owns DB row types and write operations

## Recent Progress

Recent refactors have improved dependency direction without broad reorganization:

- canonical validation now checks only canonical NBA invariants
- ingest workflows now return canonical artifacts only
- WOWY-specific input shaping moved out of `nba` and into the WOWY package
- DB-backed canonical scope loading now lives in `data`; `nba/prepare.py` only resolves scope and opponent expansion
- cached team-season listing and cache-presence checks now also live behind `data`

## Current Status

The codebase is not far from the target, but a few architectural problems still matter:

- the NBA source parsing, normalization, and team identity code is in good shape
- `shared` is small and mostly well scoped
- metric packages already have some useful internal separation
- canonical validation is now metric-agnostic, and ingest returns canonical artifacts only
- `nba` no longer imports WOWY code; WOWY input shaping now lives in the WOWY package
- `nba/prepare.py` no longer performs repository reads directly; DB-backed canonical scope loading is now behind `data`
- web callers now get cached team-season metadata from `data`, and `nba/cache_sync.py` uses a data-owned cache-presence check
- WOWY `WowyPlayerSeasonRecord` -> `PlayerSeasonMetricRow` mapping now lives in `data`
- RAWR observation building, scope filtering, and result minute-shaping now live in `apps/rawr/inputs.py`; `apps/rawr/data.py` is narrower
- RAWR metric-native `RawrPlayerSeasonRecord` preparation now lives in `apps/rawr/records.py`
- RAWR `RawrPlayerSeasonRecord` -> `PlayerSeasonMetricRow` mapping now lives in `data`
- web filter-payload shaping now lives in `web/metric_queries.py`; `web/app.py` is a bit closer to routing and request parsing only
- custom metric-query branching now lives in `web/metric_queries.py`; `web/app.py` no longer chooses metric-specific query builders directly

The main remaining issues are:

1. web modules are too orchestration-heavy
   - especially `web/app.py` and `web/metric_queries.py`
   - `web/app.py` no longer owns filter-payload shaping or custom metric-query branching, but route handlers still duplicate request-to-query wiring
2. a few large modules have become mixed-responsibility files
   - examples: `data/player_metrics_db.py`, `data/db_validation.py`, `apps/rawr/data.py`

## Next Refactors

Do these in order. Prefer medium PRs.

### 1. Finish cleanup inside metric packages

WOWY:

- keep math in `analysis.py`
- keep canonical -> WOWY conversion in `derive.py` or `inputs.py`
- keep metric-native derived records in `records.py`
- keep CLI/report wiring in `service.py`
- `data` now owns WOWY persistence-row mapping

RAWR:

- continue shrinking `apps/rawr/data.py`
- `apps/rawr/inputs.py` now owns observation building and input/result shaping
- `apps/rawr/records.py` now owns metric-native player-season record construction
- `data` now owns RAWR persistence-row mapping
- keep CLI/report orchestration in `service.py`

### 2. Shrink the web layer

Target:

- `web/app.py` handles routing and request parsing
- `web/metric_queries.py` continues splitting toward smaller query/payload helpers
- `web/metric_store.py` only orchestrates cached-store refreshes

Do not let the web layer become the place where all cross-layer glue accumulates.

### 4. Rename `apps` to `metrics`

Do this after the boundary cleanup, not before.

Target:

- `src/wowy/apps/wowy` -> `src/wowy/metrics/wowy`
- `src/wowy/apps/rawr` -> `src/wowy/metrics/rawr`

Rename only after package ownership is clearer.

## Working Rules For Future Refactors

When changing structure:

- prefer moving code to the layer that already owns the concept
- prefer deleting glue code over preserving weak abstractions
- prefer pure functions over multi-role service modules
- keep package `__init__.py` files minimal
- do not add new cross-layer shortcuts just because they are convenient
- do not preserve a misplaced abstraction just because it already exists
- do not delete useful behavior until its new owner is clear

When unsure where code belongs:

- if it describes canonical basketball truth, it belongs in `nba`
- if it reads or writes SQLite rows, it belongs in `data`
- if it computes a derived metric, it belongs in `metrics`
- if it shapes HTTP payloads or route behavior, it belongs in `web`

## Update Protocol

After a major refactor:

1. update "Current Status" to reflect what is still true
2. delete completed items from "Next Refactors"
3. rewrite the remaining steps so the next Codex instance can continue without rereading old work

If this file starts feeling long, shorten it. The goal is direction, not exhaustiveness.
