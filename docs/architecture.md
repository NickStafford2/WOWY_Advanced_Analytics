# Architecture

`docs/redesign_handoff.md` is the active instruction doc for the current boundary redesign.
If this file and that file disagree, follow `docs/redesign_handoff.md`.

## Goal

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

## Persistence Boundary

Derived metrics should not save themselves.

The intended direction is:

1. `metrics` computes derived outputs, owns metric-native records
2. `data` persists those outputs and owns DB row types and write operations.


## Current Refactor Direction

The active rebuild direction is:

1. create a stable service/application boundary
2. move metric-specific shaping out of `data/`
3. standardize typed public contracts
4. split mixed-responsibility metric-store code
5. tighten import boundaries

See `docs/redesign_handoff.md` for the concrete execution order and update protocol.

## Ordered Refactor Plan

Refactor in this order. The sequence matters because later cleanup gets much easier once the outer boundary is stable.

### 1. Make `services` the real outer boundary

Do this first because it gives the rest of the repo one place to depend on.

- `web`, CLI, and scripts should import from `services` only.
- `services` should own request parsing, use-case orchestration, and high-level result types.
- `services` should stop reaching into private `data` modules such as `_paths`.

Why first:

- it shrinks the public surface area immediately
- it stops new deep imports from spreading
- it gives later refactors a stable target to move code behind

Current examples to fix early:

- `src/rawr_analytics/web/app.py` imports `metrics.constants` and `metrics.metric_query` types directly
- `src/rawr_analytics/services/rebuild.py` imports `rawr_analytics.data._paths` and `rawr_analytics.data.db_validation`

### 2. Pull orchestration out of deep modules and into small service use cases

Once `services` is the outer boundary, move multi-step coordination there.

- `services` should coordinate ingest, refresh, rebuild, and query flows
- `workflows` should either disappear or become very small internal helpers
- use cases should compose public package APIs instead of deep sibling imports

Why second:

- orchestration is where cross-package leakage happens
- moving it early clarifies what each lower package actually needs to expose

Current examples:

- `src/rawr_analytics/workflows/nba_ingest.py` coordinates source loading, normalization, validation, and persistence in one place

### 3. Split pure metric computation from metric store access

After orchestration moves up, separate derived computation from cached view building.

- `metrics.rawr` and `metrics.wowy` should mostly accept typed canonical inputs and return typed metric outputs
- database reads, scope freshness checks, and cached leaderboard loading should live in `data` or `services`, not inside metric view modules
- metric packages should own metric formulas, defaults, filters, and result models

Why third:

- right now `metrics` is both computing values and coordinating storage-backed queries
- this is the main reason the metric boundary still feels large and blurry

Current examples:

- `src/rawr_analytics/metrics/metric_query/views.py` mixes store reads with metric-specific shaping
- `src/rawr_analytics/metrics/metric_query/scope.py` mixes UI filter payload building with cache/store freshness checks
- `src/rawr_analytics/metrics/rawr/dataset.py` and `src/rawr_analytics/metrics/wowy/dataset.py` reach directly into `data`

### 4. Tighten the `data` package into explicit repository/query APIs

Once callers stop reaching into internals, simplify `data` around a few public entrypoints.

- each `data` subpackage should expose a small public API through `__init__.py`
- private helpers such as `_paths` should stay private
- scope key construction and validation helpers should live behind a clearer public contract if they are needed outside `data`
- row types should stay close to the repository modules that load and store them

Why fourth:

- it is easier to design the data API after the real callers have been simplified
- otherwise the repo risks preserving accidental helper functions as public contracts

### 5. Collapse redundant wrapper layers

After the boundaries are clearer, delete forwarding code that no longer helps.

- remove service wrappers that only rename one function and pass arguments through
- remove modules that exist only to bounce imports around
- keep wrappers only when they enforce a boundary or simplify a data contract

Why fifth:

- wrapper cleanup is safer after responsibilities are settled
- doing this too early can create churn without improving architecture

### 6. Shrink and police `shared`

Do this continuously, but enforce it harder once the main seams are in place.

- keep only truly cross-cutting primitives in `shared`
- move NBA-specific and metric-specific helpers back to their owning subpackages
- prefer duplication over turning `shared` into a second dumping ground

Why last:

- `shared` becomes easier to judge once package ownership is clearer

## First Concrete Moves

If starting immediately, do these in order:

1. Add service-owned public functions and result types so `web/app.py` can stop importing from `metrics` directly.
2. Move rebuild filesystem and validation operations behind public `data` functions so `services/rebuild.py` stops importing private `data` modules.
3. Move the ingest orchestration from `workflows/nba_ingest.py` behind a service-facing use case built from `nba` and `data` public APIs.
4. Split `metrics/metric_query` into two concerns:
   one module for metric query building and one module for store-backed presentation/query services.
5. Replace remaining direct imports of `data.game_cache.repository`, `nba.source.*`, and `data.metric_store_scope` from outer layers with narrower public APIs.

## Working Rules For Future Refactors

When changing structure:

- prefer deleting glue code over preserving weak abstractions
- prefer pure functions over multi-role service modules
- keep package `__init__.py` files minimal
- treat tests and scripts as public-API consumers; if they import a deep module, that module is effectively public until the imports are changed
- do not add new cross-layer shortcuts just because they are convenient
- do not preserve a misplaced abstraction just because it already exists

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
4. Suggest a simple commit message for me to use. 

If this file starts feeling long, shorten it. The goal is direction, not exhaustiveness.
