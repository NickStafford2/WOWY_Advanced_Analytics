- The biggest structural issue is repeated validation spread across too many layers. 
Metric filter checks live in 
  [metrics/rawr/inputs.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/metrics/rawr/inputs.py#L7) and 
  [metrics/wowy/inputs.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/metrics/wowy/inputs.py#L7), 
shared filter checks live in 
  [shared/filters.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/shared/filters.py), 
canonical game validation lives in 
  [nba/normalize/validation.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/nba/normalize/validation.py#L20), 
DB-level validation lives in 
  [data/game_cache/validation.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/game_cache/validation.py#L31) plus 
  [data/player_metrics_db/validation.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/player_metrics_db/validation.py#L20). 
The same invariants are being re-expressed instead of flowing from one explicit contract per layer.


- Package boundaries are still too porous. 
[data/rawr.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/rawr.py#L7) 
reaches into metric internals, including private helpers from 
  [metrics/rawr/_observations.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/metrics/rawr/_observations.py). 
  [metrics/metric_query/views.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/metrics/metric_query/views.py#L5) 
depends directly on data-store query modules and also on custom query builders in 
  [data/rawr.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/rawr.py) and 
  [data/wowy.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/wowy.py). 
That means metrics is not purely metric logic and data is not purely persistence.




- There is clear API drift from the rebuild. 
  [data/game_cache/validation.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/game_cache/validation.py#L87) 
calls _validate_canonical_game(...) with expected_team_id and expected_season_type, but the current function in 
  [nba/normalize/validation.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/nba/normalize/validation.py#L82) 
takes expected_team: Team and expected_season: Season. That is a concrete sign that some modules still target an older contract.


- Public interface conventions are inconsistent across subpackages. metrics/rawr exports a small surface in 
  [metrics/rawr/__init__.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/metrics/rawr/__init__.py#L3), 
while metrics/wowy exports many models in 
  [metrics/wowy/__init__.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/metrics/wowy/__init__.py#L3). 

[nba/source/__init__.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/nba/source/__init__.py#L8) 
publicly re-exports from a private _load.py module. 
[nba/normalize/validation.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/nba/normalize/validation.py#L169) 
exports _canonical_team_abbreviation in __all__ even though it is private by naming. 
The naming rules and file patterns are not being applied consistently.



- Several modules still use loose dict[str, Any] payloads where a small explicit contract would help the rebuild. 
[data/rawr.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/rawr.py#L110), 
[data/wowy.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/wowy.py#L90), and 
[metrics/metric_query/views.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/metrics/metric_query/views.py#L24)
all build ad hoc dictionaries for queries and view payloads. That makes the seams harder to stabilize and encourages duplicated shaping logic.



- File patterns are uneven. data has real subpackages like 
  [data/game_cache](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/game_cache) and 
  [data/player_metrics_db](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/player_metrics_db), 
  but also a flat family of metric_store*.py modules. 

nba mixes flat modules with nested source and normalize. 
metrics uses subpackages, but each one follows different export rules. 
The repo feels like multiple refactor styles coexisting.

What Looks Best

- workflows is relatively close to the boundary shape you want. 
  [workflows/nba_ingest.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/workflows/nba_ingest.py#L27) and 
  [workflows/nba_cache.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/workflows/nba_cache.py) 
at least read like orchestration entrypoints with explicit request/result types.

- The package docstrings in 
  [data/__init__.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/data/__init__.py), 
  [metrics/__init__.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/metrics/__init__.py), and 
  [nba/__init__.py](/home/nick/Projects/RAWR_Analytics/src/rawr_analytics/nba/__init__.py) 
describe a clean separation. The codebase is not fully aligned with that separation yet, but the target is already stated.
