# Refactor Plan

## Goal

Shrink package public interfaces, reduce cross-package imports, and make internal helpers private by default.

## Order

1. Split `data/player_metrics_db.py` into smaller private modules.
   Separate row models, repository reads/writes, store refresh helpers, and validation helpers.

2. Remove cross-module imports of private helpers.
   `data/db_validation.py` should stop reaching into private functions from `player_metrics_db.py` and `data.game_cache.validation`.

3. Add stable metric package entrypoints.
   `web` should depend on a small set of `metrics.rawr` and `metrics.wowy` entrypoints instead of importing `records.py`, `analysis.py`, and other internals directly.

4. Re-check package exports.
   Keep `__init__.py` files small, decide which symbols are intentionally public, and rename or hide internal-only modules where useful.

## Rule Of Thumb

If a module is only imported inside one package, treat it as internal. If `tests/` or `scripts/` import a deep module path, that path is part of the effective public surface until those imports are moved to a package-level API.
