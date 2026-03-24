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

- fetch raw payloads into `data/source`
- parse raw payloads into source models
- normalize source models into canonical records
- validate the canonical team-season batch once
- persist validated rows to SQLite

Current module split:

- `ingest/cache.py`: fetch and cached payload management
- `ingest/parsers.py`: raw payload to source-model parsing
- `ingest/source_rules.py`: known source-row classifications and raw numeric/minutes parsing
- `ingest/normalize.py`: source-model to canonical-record normalization
- `ingest/runner.py`: team-season orchestration, failure aggregation, persistence handoff
- `ingest/validation.py`: canonical batch invariants and post-normalization consistency checks
- `ingest/__init__.py`: public ingest package entrypoint
- `team_identity.py`: stable team ID and alias reconciliation

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
