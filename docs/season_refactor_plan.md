# Season Refactor Plan

## Boundary Rule

`seasons=None` should eventually disappear from most of the core metric path.

It is acceptable to rebuild derived data after this refactor. Preserving
existing metric-store snapshot compatibility is not required if rebuilding from
the normalized cache is simpler and cleaner.

Right now it is carrying too many meanings:

- all cached seasons for one `season_type`
- all seasons already present in a metric snapshot
- no explicit season filter in a UI payload

That is a boundary smell. One value is representing multiple concepts.

## Target Rule

- External request/input boundaries may omit seasons.
- Query normalization should resolve omitted seasons into an explicit normalized `list[Season]`.
- Core metric/data boundaries should receive an explicit `list[Season]`, not `None`.
- Core normalized query objects should carry one `seasons: list[Season]` field only.
  Do not keep both `requested_seasons` and resolved `seasons` on the same core query object.

Ownership rule:

- request parsers may leave seasons omitted
- query builders should own omitted-season resolution
- app services and deeper layers should assume seasons are already concrete
- query builders should resolve omitted seasons from normalized cache
  availability, not from metric-store catalogs

## Implication

If a caller wants "all available seasons", that should be resolved before entering the metric engine.

In other words:

- request edge: `seasons` may be omitted
- app/query boundary: omission becomes an explicit season list
- metric/cache/data boundary: `seasons` is always concrete

If the program still needs to preserve "user did not explicitly choose seasons",
that should live in a separate outer request/filter DTO, not in the normalized
core metric query object.

## Where `None` May Still Make Sense

`None` may still be acceptable at outer boundaries where the distinction matters:

- request parsing
- presenter/filter DTOs that need to preserve "user did not select seasons"
- possibly rebuild/admin entrypoints before season selection is resolved
- lower-level storage/query helpers where `None` has one precise meaning:
  omit the season filter entirely

If a field means "no explicit user filter was provided", it should be treated as a different concept from the normalized metric query season list.

Important distinction:

- `None` means the season filter is omitted and still unresolved
- `[]` means the season set has been resolved and is empty

`[]` is a valid internal normalized value. It must not silently widen into "all
seasons". If a lower-level query helper receives `[]`, it should return no rows
or no scopes rather than dropping the season filter.

## Migration Direction

- define and stabilize `Season` identity and normalization rules first
- remove `seasons=None` branching from core metric and cache paths
- pass explicit normalized `list[Season]` into scope resolution and metric execution
- keep any optional season semantics only at the outer request/presenter layer
- update live query paths before metric-store snapshot paths
- redesign metric-store scope keys, catalog metadata, and span metadata after
  live query season handling is stable
- remove old single-`season_type` query plumbing last

## Season Identity

`Season.id` should be the canonical exact season identifier.

Examples:

- `2019-20:REGULAR`
- `2019-20:PLAYOFFS`

The NBA API year-only format should stay separate on the model as a distinct
property, because it is not a unique season identity.

Preferred property name:

- `Season.year_string_nba_api`

Examples:

- `2019-20`

Implication:

- code that needs exact season identity should use `Season.id`
- code that needs the NBA API year string should use `Season.year_string_nba_api`
- payloads and keys should not treat the NBA year string as a unique season identifier
- until mixed season-type query payloads are actually supported, outward-facing
  APIs may still emit year strings where the current contract is year-only; do
  not introduce a half-migrated outward payload scheme prematurely

Outward-facing API rule:

- when a payload needs to identify a season, it should use `season_id`
- outward-facing payloads should not use ambiguous year-only `season` values for identity
- `Season.year_string_nba_api` may still be included later for display or grouping convenience, but `season_id` is the required identifier

Internal API rule:

- internal module boundaries may pass full `Season` objects directly
- internal code should prefer passing `Season` dataclass values instead of splitting them into separate year and season-type fields unless a lower-level storage or serialization boundary requires it

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
- when a mixed query includes incomplete seasons, return results for the
  complete seasons only
- attach a warning listing the excluded incomplete seasons
- write an error log entry documenting the excluded incomplete seasons

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
- treat incompleteness as a property of the requested season itself, not of a separate season-type bucket

## Metric Store Snapshot Policy

Metric-store snapshots should only represent fixed full-history season-set
shapes.

This means:

- metric-store refresh should build snapshots only for full NBA history
- full NBA history means all exact `Season` values currently present in the
  normalized cache for the selected shape
- teams are not part of metric-store snapshot identity
- team-filtered metric queries should remain live-query only for now

For each metric, build exactly these stored season-set shapes:

- `PRESEASON`
- `REGULAR`
- `PLAYOFFS`
- `REGULAR + PLAYOFFS`
- `PRESEASON + REGULAR + PLAYOFFS`

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

## Non-Goals

- no team-scoped metric-store snapshots
- no arbitrary stored exact season subsets
- no outward-facing year-only `season` identifiers where exact season identity
  is required
- no long-lived mixed old/new season identity scheme during the migration
