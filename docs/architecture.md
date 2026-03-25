# Architecture

## Why This Needs To Change

The current structure has grown in a way that makes the direction of data flow hard to reason about.

Right now:

- metric code consumes normalized NBA data, cache state, and database state
- some lower-level code also imports metric code
- some modules mix computation, persistence-shaped outputs, and presentation
- package names like `apps` and module names like `data.py` hide what actually owns what

This makes it hard to answer basic questions:

- What is canonical data and what is derived data?
- Which layer is allowed to depend on which other layer?
- Where should a new transformation live?
- Who owns saving derived metrics to the database?

The goal of this architecture plan is to make the system easier to reason about by making dependency direction explicit.

The target is not to remove all coupling. The target is to make coupling one-way, intentional, and easy to inspect.

## Big Picture

The program should be organized as a layered pipeline:

1. Source data is fetched and cached.
2. Source payloads are parsed and normalized into canonical NBA-domain records.
3. Canonical records are persisted and queried through the data layer.
4. Metrics are computed from canonical inputs.
5. Web and CLI surfaces consume metric outputs.

The intended dependency direction is:

`source/cache -> nba -> data -> metrics -> web/cli`

In practice, `nba` and `data` are both foundational layers, and `metrics` sits above them.

The most important architectural rule is:

- lower layers must not import higher layers

That means:

- `metrics` may depend on `nba` and `data`
- `web` and CLI may depend on `metrics`
- `nba` must not depend on `metrics`
- `data` must not depend on `metrics`

If a lower layer needs code that currently lives in a metric package, that is a sign that either:

- the code is actually canonical domain logic and should move down, or
- the lower layer should stop depending on it

## Target Layers

### `src/wowy/nba`

This layer owns canonical basketball-domain transformations and rules.

Responsibilities:

- raw source payload parsing into source models
- normalization into canonical game/player/team records
- validation of canonical normalized batches
- team identity and historical continuity rules
- canonical preparation helpers that are not metric-specific

This layer should know nothing about WOWY, RAWR, or any other derived metric.

### `src/wowy/data`

This layer owns persistence and retrieval.

Responsibilities:

- SQLite repositories and row models
- metric store persistence
- cache metadata persistence
- loading canonical rows from the database
- writing derived metric results to the database

This layer should not contain metric math.

This layer should be responsible for saving derived metric outputs, not for computing them.

### `src/wowy/metrics`

This is the future home for what is currently under `src/wowy/apps/`.

This layer owns derived analytics built on top of canonical data.

Responsibilities:

- metric-specific input shaping from canonical rows
- metric computation
- metric-specific derived record building
- metric-specific CLI/report orchestration

This layer may depend on `nba` and `data`.

This layer should not be imported by `nba` or `data`.

### `src/wowy/web`

This layer owns presentation and query endpoints.

Responsibilities:

- Flask app wiring
- payload shaping for frontend routes
- use of cached metric store rows
- custom query orchestration over metric outputs

This layer should depend on stable metric entrypoints, not on low-level helpers.

### `src/wowy/shared`

This layer owns genuinely cross-cutting helpers that are not metric-specific and not NBA-specific.

Examples:

- common filter validation
- minute utility helpers
- scope formatting
- progress helpers

Do not move code here just because the right owner is unclear. `shared` should stay small.

## Target Shape For Metric Packages

Each metric package should expose a small number of clear owner modules. It does not need to collapse to one public function.

The important thing is that each module has one primary audience and one primary responsibility.

For a metric package like `wowy.metrics.wowy`, the target shape should look like this:

- `analysis.py`
  Metric math on metric-native inputs.
  Example: `compute_wowy(...)`

- `derive.py` or `inputs.py`
  Metric-specific conversion from canonical NBA rows into metric-native inputs.
  Example: `derive_wowy_games(...)`

- `records.py`
  Stable derived outputs for web and persistence workflows.
  Examples:
  - `prepare_wowy_player_season_records(...)`
  - `build_wowy_metric_rows(...)`
  - `build_wowy_shrunk_metric_rows(...)`

- `service.py` or `cli.py`
  CLI/report orchestration only.
  Example: `prepare_and_run_wowy(...)`

- `formatting.py`
  Human-facing output formatting only, if needed.

Not every metric package must have every file. The point is to avoid mixed-responsibility grab-bags like a generic `data.py` that owns everything.

## Stable Entry Points

“Small public interface” does not mean “one function per package.”

It means a small number of stable owner modules and behavior boundaries.

For a metric package, the stable entrypoints should be the functions that serve real subsystem boundaries.

For example, for WOWY the stable entrypoints are likely to be:

- metric computation:
  - `analysis.compute_wowy`

- metric-specific input derivation:
  - `derive.derive_wowy_games`

- metric record building for web/custom query use:
  - `records.prepare_wowy_player_season_records`

- metric row building for persistence:
  - `records.build_wowy_metric_rows`
  - `records.build_wowy_shrunk_metric_rows`

- CLI execution:
  - `service.prepare_and_run_wowy`

Everything else should be evaluated critically:

- if another subsystem calls it directly, it may be a stable entrypoint
- if it only supports one owner module, it should likely stay internal

## Persistence Direction

Derived metrics should not “save themselves.”

The direction should be:

1. `metrics` computes derived outputs
2. `data` persists those outputs

There are two acceptable shapes for the boundary:

- `metrics` returns metric-native records, and `data` maps them to DB rows
- `metrics` returns persistence-ready rows, and `data` writes them

The stricter long-term design is:

- `metrics` owns metric-native records
- `data` owns database row types and write operations

That means the data layer should be responsible for saving derived metrics to SQLite, even when the metric layer defines how those results are computed.

## What Is Wrong Today

The current structure has a few specific architectural smells:

- foundational layers import metric code in some places
- package roots and broad modules make ownership unclear
- some modules mix:
  - metric math
  - derived record construction
  - persistence-shaped row building
  - CLI/report output
- the name `apps` suggests user-facing applications, but these packages are actually metric engines used by the web layer and the DB refresh pipeline

The biggest structural issue is not naming. It is reversed or bidirectional dependencies between foundational layers and derived metric layers.

## Migration Plan

This should be done in stages. Do not attempt a single large rewrite.

### Stage 1: Enforce Directionality

Audit and remove imports where `nba` or `data` depend on `apps` / future `metrics`.

For each such import, decide:

- should this code move down because it is actually canonical domain logic?
- or should the lower layer stop depending on it because it is actually metric logic?

This is the most important stage.

### Stage 2: Finish Boundary Cleanup Inside Metric Packages

For each metric package:

- split mixed-responsibility modules into clear owners
- remove broad `data.py` / `service.py` grab-bags where possible
- keep internal imports pointed at concrete owner modules
- keep package `__init__.py` minimal

This has already started and should continue.

### Stage 3: Make Persistence Boundaries Explicit

Decide the exact persistence contract between `data` and `metrics`.

Preferred target:

- `metrics` returns metric-native records
- `data` converts and persists them

At minimum, persistence responsibility must be clearly owned by `data`.

### Stage 4: Rename `apps` To `metrics`

Only after the dependencies and module ownership are clearer.

At that point the rename should be mostly mechanical:

- `src/wowy/apps/wowy` -> `src/wowy/metrics/wowy`
- `src/wowy/apps/rawr` -> `src/wowy/metrics/rawr`

Do not rename first and hope the design becomes clearer afterward.

### Stage 5: Tighten Public Surfaces

After the rename:

- document stable entrypoints for each metric package
- reduce accidental helper exports
- keep tests importing concrete owner modules rather than compatibility barrels

## Practical Rule Of Thumb

When adding or moving code, ask:

- Is this canonical NBA-domain logic?
  Put it in `nba`.

- Is this persistence or repository logic?
  Put it in `data`.

- Is this derived analytics built from canonical records?
  Put it in `metrics`.

- Is this presentation or route payload shaping?
  Put it in `web`.

Then ask one more question:

- Would moving this here create an upward dependency from a foundational layer into a derived layer?

If yes, stop and rethink the placement.

## Short Version

The architecture should move toward one-way layering:

- canonical/source/persistence layers below
- metric computation above them
- web and CLI above metrics

The key change is not just renaming `apps` to `metrics`.

The key change is making sure foundational layers do not depend on derived metric layers.
