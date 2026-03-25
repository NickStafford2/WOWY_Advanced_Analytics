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
- `nba/source.cache` now owns the real source fetch/cache helpers
- `nba/normalize` now owns the real normalization and normalized validation logic
- `data/game_cache` now owns the real schema, repository, fingerprint, row, and cache-validation modules
- `workflows/nba_ingest.py` now owns the real ingest flow
- `wowy.nba.ingest` compatibility layers were removed after callers were migrated
- several clean call sites now import `data.game_cache` directly
- normalized cache reads now go through `nba.prepare` read-only loaders
- WOWY and RAWR now read normalized cache only
- WOWY and RAWR metric refresh/build paths now go through metric-specific data modules
- `canonical_*` loader/artifact names in the normalized-cache read path were renamed to `normalized_*`

Still temporary:

- `web/service.py` still mixes metric refresh orchestration and metric query/read payload building

## Next

1. Split `web/service.py` into clearer refresh/build vs read/query responsibilities.
2. Remove any remaining low-value compatibility args or aliases only when they no longer help staged call-site migration.

## Constraints

- Prefer deleting compatibility glue instead of adding more.
- Keep changes staged and runnable.
- Do not preserve legacy structure at the expense of clarity.
