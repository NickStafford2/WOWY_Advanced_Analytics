# Architecture

## `apps/`

Application-specific analysis flows live under `src/wowy/apps/`.

- `apps/wowy/` contains the baseline WOWY analysis, formatting, and CLI entrypoint
- `apps/rawr/` contains the RAWR-specific data shaping, model fitting, formatting, and CLI entrypoint

If you add a new user-facing analysis tool, it should usually get its own `apps/<tool_name>/` package.

## `nba/`

NBA-specific ingestion, cache management, and team-season scope logic live under `src/wowy/nba/`.
This layer is responsible for getting source data into the local SQLite cache used by the analysis apps.

The ingest flow is intentionally split into stages:

- parse raw source payloads into source-shaped rows
- normalize those into canonical basketball records
- validate the canonical team-season batch once
- persist validated rows to SQLite

Team identity reconciliation and historical alias handling should stay centralized in this layer rather than being repeated across parse, normalize, validation, and DB code.

## `data/`

Database access and explicit export helpers live under `src/wowy/data/`.
This layer should stay focused on storage and I/O mechanics rather than analysis rules.

## `shared/`

Cross-app helpers live under `src/wowy/shared/`.

Use this for logic that is not specific to one analysis path, such as common filters, minute summaries, or scope formatting.

## Rule of thumb

- Put model-specific logic in `apps/`
- Put NBA cache and preparation logic in `nba/`
- Put database and export I/O mechanics in `data/`
- Put reusable cross-app helpers in `shared/`
