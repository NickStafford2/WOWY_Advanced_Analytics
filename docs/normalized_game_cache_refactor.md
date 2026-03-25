# Normalized Cache Refactor

## Goal

Keep these boundaries explicit:

- `nba/source`: payload-shaped source parsing and source rules
- `nba/normalize`: normalized basketball domain models and batch validation
- `data/game_cache`: normalized cache persistence and persisted-state validation
- `workflows/nba_ingest.py`: ingest orchestration

## Progress

Done:

- `nba/source` now owns the real source models, parsers, and rules
- `nba/normalize` now owns the real normalization and normalized validation logic
- `data/game_cache` now owns the real schema, repository, fingerprint, row, and cache-validation modules
- `workflows/nba_ingest.py` now owns the real ingest flow
- `wowy.nba.ingest.runner` and `wowy.data.game_cache_db` were reduced to compatibility layers
- several clean call sites now import `data.game_cache` directly

Still temporary:

- `wowy.nba.ingest.{parsers,source_rules,normalize,validation,runner}`
- `wowy.data.game_cache_db`

## Next

1. Repoint remaining safe callers away from compatibility modules.
2. Separate normalized cache refresh from derived metric refresh so WOWY and RAWR only read normalized cache and write through metrics-specific data modules.
3. Remove leftover `canonical_*` naming and compatibility wrappers only after the new boundaries are stable.

## Constraints

- Prefer deleting compatibility glue instead of adding more.
- Keep changes staged and runnable.
- Do not preserve legacy structure at the expense of clarity.
