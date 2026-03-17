# Architecture

## `apps/`

Application-specific analysis flows live under `src/wowy/apps/`.

- `apps/wowy/` contains the baseline WOWY analysis, formatting, and CLI entrypoint
- `apps/rawr/` contains the RAWR-specific data shaping, model fitting, formatting, and CLI entrypoint

If you add a new user-facing analysis tool, it should usually get its own `apps/<tool_name>/` package.

## `nba/`

NBA-specific ingestion, cache management, file path conventions, and team-season scope logic live under `src/wowy/nba/`.
This layer is responsible for getting source data into the local normalized cache and preparing derived files needed by the analysis apps.

## `data/`

CSV loading, writing, and combine utilities live under `src/wowy/data/`.
This layer should stay focused on file formats and I/O mechanics rather than analysis rules.

## `shared/`

Cross-app helpers live under `src/wowy/shared/`.

Use this for logic that is not specific to one analysis path, such as common filters, minute summaries, or scope formatting.

## Rule of thumb

- Put model-specific logic in `apps/`
- Put NBA cache and preparation logic in `nba/`
- Put file I/O and CSV combination logic in `data/`
- Put reusable cross-app helpers in `shared/`
