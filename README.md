# RAWR Analytics

Python project for experimenting with historical basketball impact metrics. Designed for coarse game level analytics for cross generation player comparisons.

## Overview

This repo has two game-level analysis paths backed by SQLite:

- WOWY: a simple with/without baseline on derived game records
- RAWR: a ridge regression on validated canonical game and player rows

The current web app goal is player comparison across the full cached history span. The primary WOWY leaderboard should surface the strongest multi-season player profiles; team filters only narrow the sampled games.

## Commands

Install dependencies:

```bash
poetry install
```

Run WOWY:

```bash
poetry run wowy
```

Run RAWR:

```bash
poetry run rawr
```

Run the Flask backend for the web prototype:

```bash
poetry run wowy-web
```

Show CLI usage:

```bash
poetry run wowy --help
poetry run rawr --help
```

Run tests:

```bash
poetry run pytest
```

The React frontend lives in `frontend/` and can be started there with `npm run dev`.

Examples:

```bash
poetry run wowy --season 2024-25 --team BOS --top-n 25
poetry run rawr --season 2024-25 --ridge-alpha 1.0 --top-n 25
poetry run rawr --season 2024-25 --team BOS --ridge-alpha 1.0 --min-games 20 --min-average-minutes 15 --min-total-minutes 500
poetry run rawr --season 2024-25 --team BOS --tune-ridge
```

Both CLIs read cached team-season data from `data/app/player_metrics.sqlite3`. If you request a specific scope with `--season` and optionally `--team`, missing team-season data is fetched into the database automatically.

TODO: `--team` without `--season` still depends on already-cached seasons. Align that with the intended eager-fetch behavior.

## Cache tools

Fetch and cache one season:

```bash
poetry run python scripts/cache_season_data.py 2024-25
```

Fetch many seasons:

```bash
poetry run python scripts/cache_all_seasons.py --start-year 2024 --first-year 2022
```

Runtime analysis and the web app only depend on two live project data stores: the source cache under `data/source` and the SQLite app store under `data/app`.

NBA ingest rules:

- Empty cached payloads are invalid and must be refetched, not normalized.
- `BoxScoreTraditionalV2` can be empty for newer games; ingest retries with `BoxScoreTraditionalV3`.
- Team identity is keyed by stable source team IDs, with abbreviation aliases reconciled centrally.

Implementation details for ingest stages and module boundaries live in [docs/architecture.md](docs/architecture.md).

## Output notes

WOWY output includes player id or name, minute context, with/without samples, average margins, and score.

RAWR output includes observation count, fitted player count, intercept, home-court estimate, and ranked player coefficients.

## More detail

- [docs/models.md](docs/models.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/roadmap.md](docs/roadmap.md)
