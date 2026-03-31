# Redesign Handoff

This file is the working instruction doc for the current rebuild.

Use it as the source of truth for package-boundary work until the redesign is complete.
When a future Codex turn finishes a meaningful piece of this plan, that turn should:

1. update this file to reflect the new current state
2. update [docs/refactor_plan.md](/home/nick/Projects/RAWR_Analytics/docs/refactor_plan.md) with what was actually completed
3. remove stale instructions instead of stacking new notes on top of old notes

## Problem Summary

The project direction is good, but the package boundaries are still unstable.

Current problems:

- top-level scripts and web routes still assemble lower-level pieces directly
- `data/` owns orchestration and metric-specific shaping instead of only persistence and retrieval
- metric response shapes are inconsistent across RAWR and WOWY
- metric-store code mixes refresh policy, computation, persistence, and web serialization
- package `__init__` files exist, but many callers still bypass them with deep imports
- at least one rebuild script still encodes stale pre-rebuild interfaces

This is why the repo keeps needing structural cleanup. There is not yet one stable application boundary that protects the rest of the code.

## Main Goal

Finish the rebuild by establishing strong package boundaries with explicit public interfaces and simpler data contracts.

Success means:

- outer layers call stable service entrypoints
- `nba/` owns canonical basketball-domain logic
- `data/` owns database persistence and retrieval only
- `metrics/` owns metric computation and metric-specific dataset shaping
- `web/` owns HTTP parsing and response serialization only
- scripts behave like public API consumers and stop reaching into internals
- cross-package imports become boring and predictable

## Non-Goals

Do not spend time on these until the boundary work is stable:

- fixing tests
- adding new metrics
- adding new product features
- preserving backwards compatibility for stale internal APIs
- adding abstraction for hypothetical future use

## Target Dependency Direction

Allowed dependency direction:

`shared -> nba -> data -> metrics -> services -> web/scripts`

Interpretation:

- `shared/` only contains truly cross-cutting value objects and helpers
- `nba/` may depend on `shared/`
- `data/` may depend on `shared/` and `nba/`
- `metrics/` may depend on `shared/`, `nba/`, and `data/`
- `services/` may orchestrate `nba/`, `data/`, and `metrics/`
- `web/` and `scripts/` may depend on `services/` and simple public package APIs

Not allowed:

- `data/` importing metric internals
- `nba/` importing `data/`, `metrics/`, `services/`, `web/`, or scripts
- `web/` reaching into lower-level private modules
- scripts rebuilding business logic instead of calling service functions

## Public Boundary Rules

These rules matter more than exact file names.

### 1. Outer layers only call public entrypoints

Allowed examples:

- `rawr_analytics.services.ingest.refresh_season_range`
- `rawr_analytics.services.metric_store.refresh_metric_store`
- `rawr_analytics.services.metric_query.build_metric_query_view`
- `rawr_analytics.services.rebuild.rebuild_player_metrics_db`
- package `__init__` exports that are explicitly meant to be public

Avoid:

- `web/` importing `data/.../queries.py`
- scripts importing metric-private helpers
- workflows importing package internals when a public package API exists

### 2. No private cross-package imports

Do not import underscore-prefixed modules or functions across package boundaries.

If another package needs it, it is public and should live in a public module with a stable name.

### 3. Public boundaries use typed contracts

Across package boundaries, prefer dataclasses or explicit models.

Avoid returning anonymous dict payloads from core logic.

Examples that should become typed:

- metric custom query result rows
- metric store refresh requests and results
- web response payloads before JSON serialization
- CSV export rows

### 4. Serialization belongs at the edge

Core layers should return typed records.
Only the web layer should turn those records into JSON-ready dicts.
Only export code should turn those records into CSV rows.

## Target Package Responsibilities

### `shared/`

Owns:

- `Season`
- `SeasonType`
- `Team`
- other small, stable, cross-cutting value types and helpers

Must stay small.

### `nba/`

Owns:

- source payload loading and parsing
- canonical normalized game and player records
- normalization and validation
- basketball-domain rules and team identity handling

Does not own:

- SQLite persistence
- metric computation
- web payload shaping

### `data/`

Owns:

- database schema
- row models
- repositories
- persistence validation tied to stored rows
- loading canonical records from the database
- writing derived metric rows to the database

Does not own:

- RAWR and WOWY dataset assembly
- completeness policy for metric execution
- metric response payloads
- endpoint-oriented query dicts

### `metrics/`

Owns:

- metric-native input models
- metric-native output models
- metric computation
- metric-specific shaping from canonical loaded records into metric inputs

Examples:

- WOWY game derivation from canonical records
- RAWR observation derivation from canonical records
- metric-specific default filters and validation

### `services/`

This package should be introduced as the stable application boundary.

Owns:

- orchestration across packages
- application-level request and result models
- rebuild/refresh/query flows used by scripts and web

Expected modules:

- `services/ingest.py`
- `services/metric_store.py`
- `services/metric_query.py`
- `services/rebuild.py`

Exact names can change if the responsibilities stay clean.

### `web/`

Owns:

- request parsing
- HTTP error mapping
- JSON and CSV serialization
- Flask routing

Does not own:

- metric query assembly logic
- metric-store freshness policy
- direct repository access

## File-Naming Direction

Prefer names that describe stable concepts, not implementation tactics.

Better names:

- `repository.py`
- `catalog.py`
- `snapshot.py`
- `refresh.py`
- `query.py`
- `serializer.py`
- `records.py`

Weaker names:

- `views.py`
- `build_models.py`
- `common.py`
- `utils.py`

Do not rename files just for aesthetics. Rename when it makes the boundary clearer.

## Phased Execution Order

Do these in order.

### Phase 1: Create the service boundary

Status: completed.

Completed outcomes:

- introduced `rawr_analytics.services` as the public application boundary
- added typed request/result models for ingest refresh, metric-store refresh, metric-query execution, and rebuild orchestration
- moved `scripts/cache_season_data.py` to `services.ingest`
- moved `scripts/rebuild_player_metrics_db.py` to `services.rebuild`
- moved Flask routes in `web/app.py` to `services.metric_query`
- moved `web/cli.py` and `web/refresh_cli.py` to `services.metric_store`
- removed the stale rebuild script calls to old module names and stale CLI flags

Still intentionally deferred after Phase 1:

- inner metric-store responsibilities are still split across `data/` and `metrics/`
- custom-query and cached-row payloads still use dict-heavy internal shapes
- ingest internals still live under `workflows/` and reach into lower packages directly

### Phase 2: Move metric-specific dataset shaping out of `data/`

Status: completed.

Required outcomes:

- `data/rawr.py` and `data/wowy.py` stop owning metric preparation logic
- RAWR and WOWY input shaping lives under `metrics/` or a clear metric dataset package
- `data/` only loads canonical records and persists derived rows

Good end state:

- `metrics/rawr/` owns observation and player-context building
- `metrics/wowy/` owns WOWY game derivation and player-context building

Completed outcomes:

- moved RAWR dataset loading, season completeness filtering, observation derivation, and custom-query row assembly into `metrics/rawr/dataset.py`
- moved WOWY dataset loading, game derivation, player-context building, and custom-query row assembly into `metrics/wowy/dataset.py`
- exported the new public metric entrypoints from `metrics/rawr/__init__.py` and `metrics/wowy/__init__.py`
- rewired `data/metric_store.py`, metric CLIs, RAWR tuning, and metric-query views to consume the metric-owned entrypoints
- deleted `data/rawr.py` and `data/wowy.py` after moving their remaining Phase 2 responsibilities out
- kept database-facing metric row contracts on `season_id`; custom-query core payloads now also use `season_id`, while web span-point serialization still uses `season` at the HTTP edge

### Phase 3: Standardize typed request and response contracts

Required outcomes:

- one consistent custom-query row model across metrics
- one consistent cached row model across metrics
- no metric-specific dict shape drift like `season` vs `season_id`
- web serialization happens at the edge, not inside metric or data code

This phase should remove `dict[str, Any]` from core public interfaces wherever practical.

Constraint:

- do not force all metrics into one abstract filter or variable model
- metric-specific filters and variables may stay metric-specific when the metric logic genuinely differs
- only standardize the shared contract surface that callers actually consume across package boundaries
- prefer a small number of explicit typed models over a generic abstraction layer that hides real differences between metrics

### Phase 4: Split the metric-store god module by responsibility

Required outcomes:

- separate refresh orchestration from repository writes
- separate freshness validation from payload shaping
- separate full-span aggregation from DB persistence

Plausible end state:

- `data/player_metrics_db/...` remains repository-focused
- `services/metric_store.py` owns refresh orchestration
- `services/metric_query.py` owns application query flows
- `web/serializers/...` owns JSON/CSV shaping

### Phase 5: Tighten and enforce import boundaries

Required outcomes:

- callers use public modules
- deep imports are removed where a public API exists
- stale wrapper APIs are deleted instead of preserved forever

If useful after the structure settles, add lint rules or review rules to reject deep cross-package imports.

## Specific Current Problems To Fix

These are already known and should be resolved during the redesign:

1. `scripts/rebuild_player_metrics_db.py` still uses stale module names and stale CLI flags.
2. RAWR and WOWY custom query rows do not share one stable contract.
3. `data/metric_store.py` currently owns too many responsibilities.
4. metric-store fingerprint logic is duplicated and should become one shared implementation.
5. workflow and web code still bypass package public APIs with deep imports.

## What Good Looks Like

A future contributor should be able to answer these questions quickly:

- If I want to rebuild data, which service function do I call?
- If I want metric rows for the web app, which service function do I call?
- If I want to load canonical stored games, which repository function do I call?
- If I want to compute RAWR inputs from canonical records, which metric module owns that?
- If I want JSON for the frontend, which serializer owns that?

If those answers are not obvious, the redesign is not done.

## Change Discipline

For each Codex turn:

- prefer finishing one boundary slice over starting many partial moves
- delete obsolete code paths after replacing them
- do not leave duplicate interfaces active unless there is a short-term migration reason
- update docs when the architecture meaningfully changes
- treat scripts and docs as public consumers

## Update Protocol For Future Codex Turns

After completing a meaningful step:

1. update the "Current State" section below
2. update the "Completed" list below
3. update the "Next Step" section so the next Codex turn has one clear priority
4. update [docs/refactor_plan.md](/home/nick/Projects/RAWR_Analytics/docs/refactor_plan.md) with what was actually done

Keep this file concise. Delete stale notes instead of appending history.

## Current State

- Phase 1 is complete. Scripts and web now call `rawr_analytics.services` entrypoints.
- `services/rebuild.py` is now the stable rebuild boundary for ingest refresh, metric-store refresh, and validation.
- `services/metric_query.py` is now the stable query boundary used by Flask routes.
- Phase 2 is complete. Metric-specific dataset shaping now lives under `metrics/rawr/` and `metrics/wowy/`.
- Metric response contracts are still inconsistent and still rely on dict payloads internally.
- The metric-store internals are still too coupled inside `data/metric_store.py`, `data/metric_store_query.py`, and `data/metric_store_views.py`.

## Completed

- Introduced the `services/` package as the stable outer application boundary.
- Rewired top-level scripts and Flask routes to consume service entrypoints instead of lower-level internals.
- Repaired the rebuild entrypoint so it uses current package names and current service interfaces.
- Moved RAWR and WOWY metric-specific dataset shaping and custom-query assembly out of `data/` and into public metric-owned modules under `metrics/`.
- Deleted the old `data/rawr.py` and `data/wowy.py` modules after rewiring their callers.

## Next Step

Implement Phase 3.

Replace dict-heavy metric query payloads with typed core contracts before web serialization.
Start with the custom-query and cached-row payloads so RAWR and WOWY stop drifting on keys like `season` versus `season_id`.
Core and DB-facing contracts should use `season_id`; any plain `season` field should be edge serialization only.
