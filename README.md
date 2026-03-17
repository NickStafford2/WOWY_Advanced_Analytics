# WOWY Advanced Analytics

Python project for experimenting with historical basketball impact metrics. Designed for coarse game level analytics for cross generation player comparisons.

## Overview

The repository currently has two analysis paths built on game-level NBA data:

- WOWY baseline on derived `games.csv`
- RAWR on normalized game and player rows

The current web app goal is player comparison over the full cached history.
The primary WOWY web ranking is the strongest multi-season WOWY profile across that full history, with team filters used only to restrict the underlying game sample when requested.
Supporting stats like minutes and with/without counts are context for the ranked players, not the main ranking signal.

WOWY is a simple presence model:

`wowy_score = average margin when player played - average margin when player did not play`

RAWR, short for Real Adjusted WOWY Regression, is a separate game-level model with:

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
poetry run wowy --season 2020-21 --season 2021-22 --season 2022-23 --season 2023-24 --season 2024-25 --export-player-seasons data/combined/wowy/player_seasons.csv
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

Rebuild combined RAWR inputs from normalized files:

```bash
poetry run python -m wowy.data.combine_cli
```

## Output notes

WOWY output includes player name or id, minute summaries, with/without samples, average margins, and score.

For the web app, treat the primary WOWY ranking differently from a pooled all-games with/without estimate:

- Primary web ranking: multi-season WOWY profile across the full cached history span
- Supporting web columns: minutes, with/without samples, average margins, and other context
- Non-goal: replacing the main leaderboard with noisy pooled with/without rankings that push role players with tiny samples above long-term stars

RAWR output includes observation count, fitted player count, intercept, home-court estimate, and ranked player coefficients.

Player-season WOWY export:

- `poetry run wowy --season ... --export-player-seasons data/combined/wowy/player_seasons.csv`
- `poetry run python scripts/plot_wowy_player_history.py --input data/combined/wowy/player_seasons.csv --season 2020-21 --season 2021-22 --season 2022-23 --season 2023-24 --season 2024-25 --top-n 10 --min-seasons 3 --output data/combined/wowy/player_history.png`

## Interpretation

WOWY is a baseline, not an adjusted impact metric.

The RAWR rating is more adjusted than WOWY, but it is still a coarse game-level model rather than a possession-level RAPM implementation.

More detail:

- [docs/models.md](docs/models.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/roadmap.md](docs/roadmap.md)
