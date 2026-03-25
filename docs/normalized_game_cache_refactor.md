# Normalized Cache Refactor

## Goal

Separate three concerns that are currently still too coupled:

- `source`: payload-shaped data from `nba_api` and local source cache files
- `normalized`: canonical basketball domain records used by the app
- `data/game_cache`: SQLite persistence for normalized records and cache-load metadata

The project already moved from `canonical` toward `normalized`, but the boundary is still incomplete. The next refactor should make the directory layout reflect the actual pipeline:

1. fetch raw payloads
2. parse raw payloads into source models
3. normalize source models into normalized domain models
4. validate the normalized batch once
5. persist normalized rows to the game cache
6. derive WOWY and RAWR outputs from normalized cache data in app-specific layers

## Target Layout

```text
src/wowy/
  nba/
    source/
      __init__.py
      models.py
      fetch.py
      cache.py
      parsers.py
      rules.py
    normalize/
      __init__.py
      models.py
      normalize_game.py
      validation.py
      batches.py
    seasons.py
    season_types.py
    team_identity.py
    team_history.py
    team_seasons.py

  data/
    game_cache/
      __init__.py
      schema.py
      rows.py
      repository.py
      validation.py
      fingerprints.py
    player_metrics_db.py
    db_validation.py

  workflows/
    __init__.py
    nba_ingest.py
    metrics_refresh.py
```

## Layer Responsibilities

### `nba/source`

Owns source-system shape and source-system facts.

Put here:

- `nba_api` fetch and cached payload loading
- raw payload parsing
- source row classification helpers
- source dataclasses like `SourceLeagueGame` and `SourceBoxScore`
- validation of required fields, raw types, and parseability

Do not put here:

- normalized team identity decisions beyond source-shape sanity checks
- SQLite code
- WOWY or RAWR derivation

### `nba/normalize`

Owns basketball meaning and canonical domain invariants.

Put here:

- normalized dataclasses like `NormalizedGameRecord`
- conversion from `Source*` models into `Normalized*` models
- team identity resolution for normalized records
- season/date/opponent/minutes/appearance invariants
- team-season batch validation

Do not put here:

- SQL
- table names
- DB row counts
- cache-load metadata
- app-specific derived outputs as stored artifacts

### `data/game_cache`

Owns normalized cache persistence.

Put here:

- schema creation and migrations for normalized cache tables
- DB row models when useful
- repository functions to write and read normalized records
- row mapping between normalized domain objects and SQLite rows
- persisted-state validation and relation checks
- cache-load fingerprints and metadata

Do not put here:

- `nba_api` payload knowledge
- source parsing
- app-level WOWY or RAWR logic

### `apps/wowy` and `apps/rawr`

Own app-specific derived outputs.

Put here:

- derivation from normalized records
- app-specific models
- ranking and analysis services

These apps may write derived tables to the metrics DB, but they should not own normalized cache ingestion.

## Main Pipeline Entrypoints

### Input boundary

`workflows/nba_ingest.py`

This module should orchestrate:

1. `nba/source/fetch.py`
2. `nba/source/parsers.py`
3. `nba/normalize/normalize_game.py`
4. `nba/normalize/validation.py`
5. `data/game_cache/repository.py`

It should be small and procedural. It owns progress reporting, failure aggregation, and staged orchestration. It should not define domain models or SQL details.

### Output boundaries

- `data/game_cache/repository.py`: normalized cache read/write boundary
- `apps/wowy/service.py`: WOWY derivation and app-facing outputs
- `apps/rawr/service.py`: RAWR derivation and app-facing outputs

## Naming Rules

Use these terms consistently:

- `source`: raw backend payload shape
- `normalized`: canonical basketball domain shape
- `row` or `persisted`: SQLite row shape

Avoid reintroducing `canonical` in new APIs. Existing public names can be migrated in stages with compatibility wrappers.

## Migration Plan

### Phase 1: Layout scaffolding

- create `nba/source`
- create `nba/normalize`
- create `data/game_cache`
- create `workflows`
- add compatibility re-exports so imports keep working

This phase should not materially change behavior.

### Phase 2: Move source concerns

- move `source_models.py` into `nba/source/models.py`
- move parsing and source rules into `nba/source`
- keep import-compatible wrappers at old paths until the codebase is updated

### Phase 3: Move normalized concerns

- move normalized models into `nba/normalize/models.py`
- move normalization logic into `nba/normalize/normalize_game.py`
- move normalized batch validation into `nba/normalize/validation.py`

### Phase 4: Split persistence

- move normalized cache SQL from `data/game_cache_db.py` into `data/game_cache/repository.py`
- move schema helpers into `data/game_cache/schema.py`
- move cache-load fingerprint helpers into `data/game_cache/fingerprints.py`
- move persisted-state validation into `data/game_cache/validation.py`

### Phase 5: Shrink ingest runner into workflow

- make `workflows/nba_ingest.py` the orchestrator
- keep `wowy.nba.ingest` as a public compatibility layer for CLIs and callers
- stop returning WOWY artifacts from ingest

### Phase 6: Separate derived metric refresh

- make WOWY and RAWR derivation read from normalized cache
- write derived outputs through metrics-specific data modules
- keep normalized cache refresh independent from metrics refresh

### Phase 7: Remove old names

- rename remaining `canonical_*` APIs to `normalized_*`
- remove compatibility wrappers after call sites are migrated

## Immediate First Steps

The safest first implementation step is:

1. add the new package layout
2. add compatibility wrappers that re-export the current implementation
3. introduce a workflow entrypoint that delegates to the current ingest runner
4. only then begin moving logic into the new packages

That keeps the project runnable while the structure becomes more explicit.

## Current Status

The following work is already done:

- `nba/source` package exists and owns the current source models, source rules, and source parsers
- `nba/normalize` package exists and owns the current normalization and normalized validation implementation
- legacy `wowy.nba.ingest.parsers`, `wowy.nba.ingest.source_rules`, `wowy.nba.ingest.normalize`, and `wowy.nba.ingest.validation` are now compatibility wrappers
- `wowy.nba.ingest.runner`, `wowy.nba.source_audit`, and `wowy.data.game_cache_db` have been partially repointed to the new package layout
- `data/game_cache` package exists, but it is still mostly a wrapper around `data/game_cache_db.py`
- `workflows/nba_ingest.py` exists as an initial orchestration entrypoint

## Next Concrete Refactor

The next session should focus on the persistence split, not more naming cleanup.

Primary target:

- break `src/wowy/data/game_cache_db.py` into real modules under `src/wowy/data/game_cache/`

Recommended sequence:

1. move schema creation and migration helpers into `data/game_cache/schema.py`
2. move `NormalizedCacheLoadRow` and any future persistence row dataclasses into `data/game_cache/rows.py`
3. move normalized cache read/write functions into `data/game_cache/repository.py`
4. move DB-specific integrity checks into `data/game_cache/validation.py`
5. keep `data/game_cache_db.py` as a compatibility wrapper until call sites are migrated
6. repoint callers to `data.game_cache` modules directly

After that:

- update `src/wowy/data/db_validation.py` to depend on `wowy.nba.normalize.validation` and `wowy.data.game_cache.validation`, not `wowy.nba.ingest.validation`
- rename leftover public `canonical_*` APIs only after the persistence split is stable

## Constraints For The Next Session

- do not overwrite unrelated local edits in dirty files
- prefer compatibility wrappers over large flag-day import rewrites
- keep the project runnable after each step
- preserve current behavior before doing semantic cleanup
- do not mix WOWY or RAWR derivation into the normalized cache persistence layer

## Copy-Paste Handoff Prompt

Use this in the next Codex session:

```text
Read /home/nick/Projects/WOWY/docs/normalized_game_cache_refactor.md first and continue that refactor.

Current state:
- nba/source and nba/normalize now contain the real implementations for source parsing/rules/models and normalized normalize/validation logic
- old wowy.nba.ingest.{parsers,source_rules,normalize,validation} modules are compatibility wrappers
- wowy.nba.ingest.runner, wowy.nba.source_audit, and wowy.data.game_cache_db have started repointing to the new structure

Your next task is the persistence split:
1. Turn src/wowy/data/game_cache/ from wrappers into real modules
2. Move schema and migration code out of src/wowy/data/game_cache_db.py into src/wowy/data/game_cache/schema.py
3. Move normalized cache repository reads/writes into src/wowy/data/game_cache/repository.py
4. Move DB-specific cache validation into src/wowy/data/game_cache/validation.py
5. Leave src/wowy/data/game_cache_db.py as a compatibility wrapper unless all call sites are safely migrated
6. Repoint clean call sites to the new data.game_cache modules

Important:
- Avoid clobbering unrelated local edits in dirty files
- Keep behavior unchanged unless required for the layer split
- Prefer small, staged moves with working compatibility imports
- Do not do a broad naming cleanup yet; finish the persistence split first

Before editing, inspect git diff on dirty files and work around them carefully.
After changes, run focused py_compile/import checks and any narrow tests you can run safely.
```
