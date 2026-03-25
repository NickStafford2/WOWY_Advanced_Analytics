# Architecture

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


## Next Refactors

Do these in order. Prefer medium PRs.

### 1. Tighten metric package entrypoints

After the rename:

- reduce direct web imports from metric internals where convenient
- prefer a smaller set of stable metric-owned entrypoints for validation, custom-query building, and cached-row construction

### 1. Continue shrinking mixed-responsibility files

Examples:

- `data/player_metrics_db.py`
- `data/db_validation.py`
- `metrics/rawr/data.py`

## Working Rules For Future Refactors

When changing structure:

- prefer deleting glue code over preserving weak abstractions
- prefer pure functions over multi-role service modules
- keep package `__init__.py` files minimal
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
