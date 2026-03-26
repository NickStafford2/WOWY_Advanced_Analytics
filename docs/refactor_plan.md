# Refactor Plan

## Goal

Shrink package public interfaces, reduce cross-package imports, and make internal helpers private by default.

## The best places to reduce complexity:

### The ingest workflow 
in src/rawr_analytics/workflows/nba_ingest.py:75. 
ingest_team_season() is doing fetch/cache access, parsing, normalization, failure aggregation, progress reporting, and summary construction in one function.

That is a real complexity sink. The simplest reduction is to split it into:
  load source payloads -> normalize one game -> aggregate season result.
  Keep the workflow thin and push basketball rules back into nba and DB writes into data.

### The “metric API + metric data + web query” 
split is too fragmented across 
  src/rawr_analytics/metrics/rawr/api.py:29,
  src/rawr_analytics/metrics/rawr/data.py:56,
  src/rawr_analytics/web/metric_queries.py:42
  src/rawr_analytics/web/metric_store.py:98. 

Right now metadata, defaults, cached-row building, custom-query building, and leaderboard shaping are spread across layers.
The cleanest cut is: each metric package should expose one small public surface for “describe metric”, “validate filters”, “run custom query”, and “build cached rows”.
Web should only call that surface, not assemble metric behavior itself.

### Scope resolution is in the wrong place. 
src/rawr_analytics/nba/team_seasons.py:23 and 
src/rawr_analytics/nba/prepare.py:13 
Both depend on cached DB state. 

That makes nba less of a canonical basketball layer and more of a mixed domain/data layer. 
Move cache-backed scope lookup into data, and keep nba for pure season/team identity rules only.

### Web request parsing is duplicated and too metric-aware 
In src/rawr_analytics/web/app.py:218. 
The app is repeatedly parsing, validating, branching by metric, then reattaching filters. 
That is porous because HTTP concerns and metric concerns are mixed. 
A single per-metric request contract would remove a lot of this branching.
### Database validation is carrying too many responsibilities 
in src/rawr_analytics/data/db_validation.py:96. It audits tables, validates cross-table relations, summarizes trends, and renders reports. That file wants to be three simpler modules:
    audit collection, 
    summary/report shaping, 
    CLI/rendering.

If you want one rule to drive the refactor: make each package own one data contract. nba owns canonical records, data owns stored rows and scope lookup from storage, metrics owns metric inputs/outputs, and web only translates HTTP to metric calls. That would let you delete a lot of glue.

## Rule Of Thumb

If a module is only imported inside one package, treat it as internal. If `tests/` or `scripts/` import a deep module path, that path is part of the effective public surface until those imports are moved to a package-level API.
