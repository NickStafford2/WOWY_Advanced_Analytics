# WOWY Advanced Analytics

Python project for experimenting with historical basketball impact metrics. Designed for coarse game level analytics for cross generation player comparisons.

## Overview

The repository currently has two analysis paths built on game-level NBA data:

- WOWY baseline on derived `games.csv`
- ridge regression on normalized game and player rows

WOWY is a simple presence model:

`wowy_score = average margin when player played - average margin when player did not play`

Regression is a separate game-level model with:

- intercept
- home-court term
- player coefficients
- team-season terms
- opponent team-season terms

Player features are minute-weighted. Ridge regularization stabilizes the fit.

## Commands

Install dependencies:

```bash
poetry install
```

Run WOWY:

```bash
poetry run wowy
```

Run regression:

```bash
poetry run regression
```

Show CLI usage:

```bash
poetry run wowy --help
poetry run regression --help
```

Run tests:

```bash
poetry run pytest
```

Examples:

```bash
poetry run wowy --season 2024-25 --team BOS --top-n 25
poetry run regression --season 2024-25 --ridge-alpha 1.0 --top-n 25
poetry run regression --season 2024-25 --team BOS --ridge-alpha 1.0 --min-games 20 --min-average-minutes 15 --min-total-minutes 500
poetry run regression --games-csv data/combined/regression/games.csv --game-players-csv data/combined/regression/game_players.csv --tune-ridge
```

Both CLIs rebuild stale derived WOWY files automatically. If you request a specific scope with `--season` and optionally `--team`, missing team-season data is intended to be fetched automatically.

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

Report cache status for one season:

```bash
poetry run python scripts/cache_season_status.py 2024-25 --teams BOS NYK
```

Rebuild combined regression inputs from normalized files:

```bash
poetry run python -m wowy.data.combine_cli
```

## Output notes

WOWY output includes player name or id, minute summaries, with/without samples, average margins, and score.

Regression output includes observation count, fitted player count, intercept, home-court estimate, and ranked player coefficients.

## Interpretation

WOWY is a baseline, not an adjusted impact metric.

Regression is more adjusted than WOWY, but it is still a coarse game-level model rather than a possession-level RAPM implementation.

More detail:

- [docs/models.md](docs/models.md)
- [docs/roadmap.md](docs/roadmap.md)
