# Season Refactor Plan

## Boundary Rule

`seasons=None` should eventually disappear from most of the core metric path.

Right now it is carrying too many meanings:

- all cached seasons for one `season_type`
- all seasons already present in a metric snapshot
- no explicit season filter in a UI payload

That is a boundary smell. One value is representing multiple concepts.

## Target Rule

- External request/input boundaries may omit seasons.
- Query normalization should resolve omitted seasons into an explicit normalized `list[Season]`.
- Core metric/data boundaries should receive an explicit `list[Season]`, not `None`.

Ownership rule:

- request parsers may leave seasons omitted
- query builders should own omitted-season resolution
- app services and deeper layers should assume seasons are already concrete

## Implication

If a caller wants "all available seasons", that should be resolved before entering the metric engine.

In other words:

- request edge: `seasons` may be omitted
- app/query boundary: omission becomes an explicit season list
- metric/cache/data boundary: `seasons` is always concrete

## Where `None` May Still Make Sense

`None` may still be acceptable at outer boundaries where the distinction matters:

- request parsing
- presenter/filter DTOs that need to preserve "user did not select seasons"
- possibly rebuild/admin entrypoints before season selection is resolved

If a field means "no explicit user filter was provided", it should be treated as a different concept from the normalized metric query season list.

## Migration Direction

- remove `seasons=None` branching from core metric and cache paths
- pass explicit normalized `list[Season]` into scope resolution and metric execution
- keep any optional season semantics only at the outer request/presenter layer

## Season Identity

`Season.id` should be the canonical exact season identifier.

Examples:

- `2019-20:REGULAR`
- `2019-20:PLAYOFFS`

The NBA API year-only format should stay separate on the model as a distinct
property, because it is not a unique season identity.

Examples:

- `2019-20`

Implication:

- code that needs exact season identity should use `Season.id`
- code that needs the NBA API year string should use the year-only property
- payloads and keys should not treat the NBA year string as a unique season identifier

## Metric Store Scope Key

Future metric-store scope identity should be:

- `team_filter + exact ordered seasons`

Canonical rule going forward:

- seasons will be normalized before key construction
- normalization will dedupe exact `Season` values
- ordering will be by NBA year first, then season type in this business order:
  `PRESEASON`, `REGULAR`, `PLAYOFFS`
- encoded season values will stay explicit rather than hashed so the key is readable and directly debuggable
- validation will compare against that canonical encoded season list rather than a single `season_type` field

## RAWR Completeness

Recommended rule:

- RAWR completeness should be defined per exact `Season`

This means completeness is attached to a concrete season value such as:

- `2019-20 REGULAR`
- `2019-20 PLAYOFFS`

Not to a broader grouped `season_type`.

Reasoning:

- mixed regular-season and playoff queries need completeness checks on the exact requested seasons
- grouping by `season_type` keeps the old boundary alive and makes mixed queries harder to reason about
- per-season completeness matches the future contract where the core metric path operates on explicit `list[Season]`

Migration direction:

- replace season-type-based RAWR cache completeness logic with exact `Season` completeness checks
- when a mixed query is requested, include only the exact seasons that are complete
- treat incompleteness as a property of the requested season itself, not of a separate season-type bucket

## Metric Store Snapshot Policy

For now, metric-store snapshots should only represent canonical query-shaped
season sets.

This means:

- metric-store refresh should build only the defined canonical rebuilt season sets
- metric-store snapshots should not be created for arbitrary exact season subsets

Examples of season subsets that should remain live-query only for now:

- `2019-20:REGULAR` + `2021-22:PLAYOFFS`
- other sparse or ad hoc exact season combinations

Future direction:

- live queries should eventually be able to request any normalized exact
  `list[Season]`
- metric-store snapshots do not need to persist every possible live query shape

## Span Semantics

Full-span metadata should be defined over exact ordered `Season` values, not
plain NBA year strings.

This means:

- span start should be an exact season such as `2019-20:REGULAR`
- span end should be an exact season such as `2020-21:PLAYOFFS`

Reasoning:

- once regular season and playoffs can coexist for the same NBA year, a plain
  year string is not precise enough for span boundaries
- span metadata should follow the same canonical exact-season ordering used for
  keys and availability
