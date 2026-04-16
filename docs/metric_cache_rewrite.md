# Metric Cache Rewrite

## Current Status

Done:

- metric queries now split into `calc_vars` and `post_calc_filters`
- canonical `MetricCacheKey` exists and is used in refresh and query paths
- exact `Season.id` values now flow through metric-store cache identity
- RAWR `ridge_alpha` is part of cache identity, not `build_version`
- most Python/store naming has been moved from `scope`/`snapshot` to `cache`
- dead `data/metric_store_scope.py` was removed

Not done:

- combined `REGULAR + PLAYOFFS` cache semantics are still incomplete
- catalog is still overbuilt and may be mostly removable
- `db_validation.py` still has a lot of old `scope_*` naming

Recommended next steps:

1. decide whether `metric_cache_catalog` should be reduced heavily or deleted
2. if catalog stays, keep only runtime-needed metadata
3. clean `db_validation.py` naming after the catalog decision

Important note:

- clarity is more important than compatibility here
- rebuilding `metric_store.sqlite3` is acceptable
- do not preserve redundant metadata just because old audit code expects it

## Goal

The metric cache should behave like a true cache.

For the same metric query:

- loading from cache and recomputing live must produce identical metric rows
- no route should silently use cached rows built from different calculation inputs
- the cache should prevent recomputing commonly requested data

This document replaces the current blurry distinction between metric-store
cached entries, cached leaderboards, and live fallback queries.

Preferred term:

- use `MetricCacheKey`

Avoid:

- `snapshot`

`snapshot` sounds temporal and vague. The important concept here is not "when"
the rows were built. The important concept is "which exact calculation inputs
produced these rows".

## Core Rule

Each metric query must be split into two parts:

- `CalcVars`: inputs that affect how metric values are computed
- `PostCalcFilters`: filters and shaping applied after metric values already exist

Only `CalcVars` participate in cache identity.

`PostCalcFilters` must never participate in cache identity.

Query flow should be:

1. parse request
2. normalize request into a typed metric query
3. split the query into `CalcVars` and `PostCalcFilters`
4. build a canonical `MetricCacheKey` from `CalcVars`
5. load cached metric rows for that key, or compute and store them
6. apply `PostCalcFilters`
7. render payload

This same pipeline should be used by:

- metric-store refresh/rebuild
- web queries
- CLI metric queries

## Query Shape

Each metric query type should explicitly contain both halves of the query.

Preferred shape:

- `RawrQuery(calc_vars, post_calc_filters)`
- `WowyQuery(calc_vars, post_calc_filters)`

Do not keep one flat query object where calculation inputs and post-calculation
filters are mixed together at the same level.

By the time a core metric query is built:

- `calc_vars` should already be concrete and normalized
- `post_calc_filters` should already be concrete and normalized

Request-edge optionality should be resolved before building the core query
objects.

## Cached Artifact

The cache stores computed metric rows, not HTTP responses.

The cached artifact should match the website's current unit of work:

- per-player, per-season metric rows

These cached rows may include supporting fields needed by the current web views,
for example:

- player id
- player name
- season id
- metric value
- games
- minutes
- any other already-computed metric-native fields needed by current views

The cache does not store:

- final JSON responses
- `top_n` slices
- presentation-specific payloads

Those are derived after the rows are loaded.

## CalcVars

`CalcVars` must include every input that can change computed metric values.

For RAWR and WOWY this likely includes:

- metric id
- metric variant
- explicit season selection
- team selection, if team selection changes the computed values
- RAWR ridge alpha
- shrinkage mode
- shrinkage strength
- shrinkage minute scale
- any future metric-specific algorithm inputs

Important rule:

- if changing a variable can change even one cached metric row value, that variable belongs in `CalcVars`

This means user-selectable algorithm knobs such as RAWR `ridge_alpha` and
shrinkage settings must affect cache identity.

Execution rule:

- only `query.calc_vars` should be passed into metric calculation entrypoints

That means:

- only `RawrQuery.calc_vars` should flow into `build_rawr_request`
- only `WowyQuery.calc_vars` should flow into the WOWY calculation request/build path

If a field is needed by the calculation code, it is a `CalcVar`.

## PostCalcFilters

`PostCalcFilters` are applied after metric rows have already been computed.

These likely include:

- `top_n`
- minimum average minutes
- minimum total minutes
- minimum games
- minimum games with
- minimum games without
- sort direction or display shaping, if later added

Important rule:

- changing a `PostCalcFilter` must not require recomputing metric rows

Application rule:

- `query.post_calc_filters` are applied only after cached or live metric rows already exist

## Metric Cache Key

`MetricCacheKey` is the canonical identity for cached metric rows.

It should be:

- deterministic
- typed before serialization
- built only from `CalcVars`
- readable enough to debug

The metric store should load rows by `MetricCacheKey`, not by a looser scope key
that ignores calculation inputs.

Recommended structure:

- metric id
- metric variant
- exact ordered season ids
- team filter
- calculation settings

Float normalization rule:

- all float values that participate in `MetricCacheKey` must be normalized to two decimal places before serialization

Examples:

- `10` becomes `10.00`
- `10.0` becomes `10.00`
- `10.004` becomes `10.00`
- `10.005` becomes `10.01`

Implementation note:

- Python `float` is already a double-precision floating-point value
- the issue is not choosing a different Python float type
- the issue is canonical float serialization for cache identity

Do not build cache keys from raw float string conversions.

Examples of season selection that should be representable:

- regular season only
- playoffs only
- regular season plus playoffs combined
- any explicit list of exact seasons if the UI later supports it

The key should use exact normalized season identities, not ambiguous year-only
season strings.

That means metric-store identities should use exact `Season.id` values such as:

- `2019-20:REGULAR`
- `2019-20:PLAYOFFS`

Not:

- `2019-20` plus a separate season-type assumption

## Separate Build Version

Keep a small build version in addition to `MetricCacheKey`.

Purpose of build version:

- invalidate cache rows when code, schema, or row shape changes

Purpose of `MetricCacheKey`:

- identify the semantic calculation query

Important rule:

- user query semantics belong in `MetricCacheKey`
- programmer invalidation belongs in `build_version`

Do not hide semantic calc inputs inside `build_version`.

## Cache System

The app currently has two different concepts:

- normalized game cache for source inputs
- metric store for derived metric rows

Target rule:

- normalized game cache is an input data cache used to build metrics
- metric store is the derived metric row cache used by web and CLI queries

The website should think in terms of the metric row cache only.

The query layer should not have a blurry "cached leaderboard vs custom query"
distinction where one path silently falls back to a different calculation
contract.

Instead:

- all metric queries use the same metric-row cache pipeline
- cache miss means compute the exact missing `MetricCacheKey`, then store it
- cached and live paths must share the same calculation code

## Catalog Note

Based on the current code, `metric_cache_catalog` does not appear to be a core
runtime concept.

Current runtime value seems limited to:

- existence checks alongside cache-entry state
- available season ids for span reads

Much of the rest looks redundant or mainly useful for audit/validation:

- `label`
- `team_filter`
- available team rows
- full-span start/end season ids

Strong suspicion:

- `metric_cache_catalog` should be replaced by a much smaller metadata record or removed
- `metric_cache_team` is probably removable
- `metric_cache_season` may also be removable if season ids can be derived from the key or rows

## Refresh Policy

`refresh_metric_store` should warm a predefined set of high-value metric
queries.

This is not a separate cache system.

Refresh should use the normal metric query workflow:

1. build a typed metric query
2. normalize it
3. extract `CalcVars`
4. build `MetricCacheKey`
5. load or compute metric rows
6. persist the retained cache entry if the key belongs in the retained set

Do not maintain a separate refresh-only build pipeline.

Default predefined refresh set:

- full NBA history
- all teams
- default RAWR ridge alpha
- regular season
- playoffs
- regular season plus playoffs combined

Apply the same idea to WOWY and WOWY shrunk using their default calculation
settings.

These predefined queries should be treated as pinned retained keys.

## Query Usage Tracking And Retention

The metric row cache must stay bounded.

Do not let the metric store grow without limit by persisting every one-off
custom query.

Keep one retained key set per metric with a fixed maximum size:

- 10 retained `MetricCacheKey` entries per metric

The retained set should consist of:

- pinned predefined query keys
- the most-used unpinned query keys for that metric, filling the remaining slots up to 10

Everything else should be treated as a live-only custom query unless it later
earns promotion into the retained set.

Rule:

- count normalized metric queries at the `CalcVars` level, not full HTTP request strings

That means:

- two requests that differ only by `top_n` should count as the same cached metric query
- two requests that differ in `ridge_alpha` should count as different cached metric queries

This requires a small persisted query-usage table keyed by canonical
`MetricCacheKey`.

The query-usage table should be updated by the normal metric query workflow.

Do not wipe query usage on refresh.

Refresh should use this table as the dynamic input for choosing retained custom
query keys.

Refresh policy should then:

1. iterate metric by metric
2. build the pinned predefined key set for that metric
3. load the most-used unpinned keys for that metric from the query-usage table
4. fill the retained set up to 10 total keys for that metric
5. warm each retained `MetricCacheKey`

Retention policy should then:

1. compute the retained query-key set per metric from the pinned predefined
   keys plus the most-used unpinned keys up to 10 total
2. remove persisted metric rows for keys outside that retained set

One-off custom queries may still be computed on demand, but they should not be
persisted into the long-lived metric row cache unless they are part of the
retained set.

The exact query-count persistence mechanism can be simple. The important part is
the contract:

- popularity is measured on calculation identity, not presentation filters
- retention is dynamic, but pruning still happens during refresh in one
  controlled step
- retention is computed per metric, not from a single cross-metric pool

## Team-Scoped Caching

Team-scoped caching is not required as a special-case policy.

Instead:

- if team selection changes the computed metric values, then team selection is part of `CalcVars`
- if a team-scoped query is common enough, it may be retained through the dynamic top-used-key path
- if team-scoped queries are uncommon, they can still be computed live on request without being persisted

This keeps the design simple:

- no separate "team cache" concept
- just a bounded set of retained `MetricCacheKey` entries

## Season Semantics

Season handling must be explicit.

The cache key must distinguish:

- regular season
- playoffs
- regular season plus playoffs combined

Do not overload a single `season_type` field if the combined case is a first-class
supported query shape.

Prefer an explicit normalized season selection inside `CalcVars`.

Important migration note:

- the current metric-store scope key already encodes exact season identities
- but some metric-store row reads and catalog data still use year-only season ids plus a separate `season_type`

That means combined regular-season and playoff cached queries are not fully
safe until the metric-store schema, catalog, row reads, and row reconstruction
all use exact season identity consistently.

This is not a tiny local fix.

It is a real metric-store migration.

## Naming

Preferred names:

- `CalcVars`
- `PostCalcFilters`
- `MetricCacheKey`
- `CachedMetricRows`

Acceptable alternatives if needed:

- `MetricCalcVars`
- `MetricQueryFilters`

Avoid:

- `snapshot`
- `scope_key` when the real meaning is full calculation identity
- `custom_query` for the live path if the same typed query model is used everywhere

## Simplification Targets

The rewrite should simplify toward these rules:

- one typed metric query model per metric family
- one shared cache pipeline
- one canonical cache identity
- one cached artifact shape
- one place where calc inputs are separated from post-calc filters

Delete or redesign anything that violates these rules, especially:

- cache reads that ignore user-selected calculation inputs
- route names that imply a cache contract different from the real one
- refresh logic that builds rows through a different path than normal queries
- cache identity models that do not fully describe the calculation

## Implementation Direction

The intended implementation direction is:

1. define typed `CalcVars` and `PostCalcFilters` for RAWR and WOWY
2. build a canonical `MetricCacheKey` serializer from `CalcVars`
3. migrate metric-store season identity to exact `Season.id` values end to end
4. route all metric-store reads and writes through that key
5. make live compute and cached compute share the same row-building pipeline
6. replace refresh-specific scope logic with pinned predefined queries plus dynamic retained-key selection
7. add query-usage tracking at the normalized `MetricCacheKey` level
8. make refresh build the retained key set, warm it, and prune everything else

## Non-Goals

This rewrite does not introduce:

- HTTP response caching
- separate route-level cache semantics
- preserving old metric-store compatibility if rebuilding is simpler

The priority is simpler contracts and correct cache identity, not backward
compatibility.
