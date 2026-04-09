# Dev notes

- Keep the project simple and readable.
- Prefer small pure functions.
- Prefer simple pytest unit tests.
- Legacy code is to be avoided. I desire understandibility and simplicity over legacy functionality
- Prefer a clean organized database in normal form.
- Keep docs brief and focused on current behavior, commands, and file formats.
- Do not add possession-level or substitution-level logic yet.
- Preserve the player-first product goal
- Treat team filters as optional scope filters on player comparison, not as a shift toward team-centric ranking.


# Project Structure Notes

## NBA ingest pipeline
- Prefer a strict pipeline with clear stages
- Persistence models should stay close to the database layer and should only be separate from domain models when the database schema meaningfully differs from the canonical domain shape.
- Centralize team identity and historical alias handling in one shared place. Do not scatter abbreviation reconciliation across ingest, normalize, validation, and DB code.
- Prefer small adapters for each backend payload shape over large multi-purpose ingest functions with mixed responsibilities.
- Major ingest reliability problem: the NBA ingest scripts frequently fail mid-run, then succeed on rerun with no code changes. This is unacceptable. Treat repeated restart-to-progress behavior as a real bug to fix, not normal operation.
- When working on ingest or scraping, prioritize durable retries, resume behavior, and clear failure reporting so full-season and multi-season runs do not require manual restarts to eventually finish.

## Database 
- The database must be treated as a high-quality canonical store, not a best-effort cache.
- If any database data is discovered to be wrong, stale, ambiguous, partially populated, or structurally invalid, stop and surface it immediately.
- Empty source payloads are invalid cached data. Discard and refetch them instead of normalizing them or rebuilding the DB from them.
- Do not keep using a bad database. Either repair/recalculate the affected scopes immediately or stop and report the exact invalid scopes.
- No bad, stale, partial, inferred, fallback, or incomplete data is allowed in `data/app/player_metrics.sqlite3`.


## Data files
- Treat `data/source` as generated or cached project data.
- Do not bulk-read files under `data/`.
- Only inspect specific files when needed, and prefer small samples, headers, or row counts.

## Notes
- The app is player-focused, not team-focused. Team filters are allowed only as a way to restrict the game sample used for player comparison.
- Any long-running script or CLI must show visible progress by default.
- Do not replace the primary web ranking with pooled or recomputed all-games WOWY unless explicitly requested.
- If there is ambiguity about ranking semantics, prefer the ranking that surfaces the strongest cross-season WOWY player histories rather than noisy role-player outliers from tiny with/without samples.
- Team identity must be determined by stable source team IDs, not by abbreviations or aliases.
