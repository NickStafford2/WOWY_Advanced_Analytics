# WOWY Advanced Analytics

Python project for experimenting with historical basketball impact metrics.

## Goal

This project experiments with two basketball impact-analysis paths built from historical NBA game data:

- a simple WOWY baseline
- a game-level ridge regression model on normalized team and player rows

The WOWY path estimates a player's impact by comparing:

- the average point differential in games when the player played
- versus the average point differential in games when the player did not play

This is intentionally a simple starting point before moving to more advanced adjusted models.

The current WOWY implementation should be treated as a baseline, not a final player evaluation model. On pooled real NBA data it is expected to be noisy and heavily confounded by team, season, and rotation context.

## Current models

### WOWY 

wowy_score = average margin when player played - average margin when player did not play

Where:
- `margin` is the final point differential from one team's perspective
- `players` is the set of NBA player ids who appeared in that game for that team

This is a game-level presence model, not a possession-level or substitution-level plus-minus model.

### Regression 

The regression path fits a game-level linear model on normalized team-game and player-minute data.

Current features include:

- intercept
- home-court term
- one coefficient per included player
- team-season effect terms
- opponent team-season effect terms

The player component is weighted by each player's share of team minutes in that game, scaled so each side sums to 5.0 lineup slots.

Ridge regularization is used to stabilize the fit.

The regression CLI currently uses:

- `min-games` as a pre-fit inclusion rule
- `min-average-minutes` and `min-total-minutes` as post-fit output filters only

That means minute thresholds do not change which player coefficients are estimated. They only determine which fitted players are shown in the final report.

The current WOWY CLI still reads a simple derived `games.csv` file, but the project now also defines a richer normalized game-level schema for future modeling work.

## Real NBA data

The real-data path uses `nba_api` to fetch NBA game-level box score data, write canonical normalized tables, and derive the same `games.csv` format above.

The WOWY model stays unchanged:

- one row per game from one team's perspective
- `margin` remains final game point differential
- `players` remains the semicolon-separated list of NBA `PLAYER_ID` values for that team

## Main commands

Run WOWY on cached data:

```bash
poetry run wowy
```

Run regression on cached data:

```bash
poetry run regression
```

Both commands use cached data by default. If you request a specific scope with `--season` and optionally `--team`, missing team-season data is fetched automatically and derived WOWY files are rebuilt automatically when stale.

Examples:

```bash
poetry run wowy --season 2024-25 --team BOS --top-n 25
poetry run regression --season 2024-25 --ridge-alpha 1.0 --top-n 25
poetry run regression --season 2024-25 --team BOS --ridge-alpha 1.0 --min-games 20 --min-average-minutes 15 --min-total-minutes 500
```

If you want to bulk-cache a season manually, keep using:

```bash
poetry run python scripts/cache_season_data.py 2024-25
```

To fetch and cache a whole season across many teams, run:

```bash
poetry run python scripts/cache_season_data.py 2023-24
```

That refreshes the cache and writes per-team normalized and WOWY files under `data/normalized/nba/` and `data/raw/nba/team_games/`.


## Install

Install dependencies with:

```bash
poetry install
```

## Tests

Run tests with:

```bash
poetry run pytest
```

## Example output

```text
WOWY results (Version 1)
------------------------------------------------------------------------
player_id     with  without     avg_with    avg_without      score
------------------------------------------------------------------------
1628369          4         2         9.00          1.00       8.00
```

## Notes on interpretation

WOWY output is a baseline comparison, not an adjusted impact estimate.

Regression output is more adjusted than WOWY, but it is still a coarse game-level model rather than a possession-level RAPM implementation. The current version is most useful for experimentation, debugging, and comparing modeling choices rather than as a finished public rating.

## Next model direction

The repository now includes a separate regression-based player analysis path built on normalized game-level data. The current WOWY score remains useful as a simple baseline and debugging reference, while future model development can extend the regression path with better regularization, tuning workflows, and richer stability analysis.
