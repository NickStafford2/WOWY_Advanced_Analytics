# Refactor Plan

## Goal

Shrink package public interfaces, reduce cross-package imports, and make internal helpers private by default.

## Rule Of Thumb

If a module is only imported inside one package, treat it as internal. If `tests/` or `scripts/` import a deep module path, that path is part of the effective public surface until those imports are moved to a package-level API.
