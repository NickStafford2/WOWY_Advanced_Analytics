# WOWY

Python project for experimenting with historical basketball impact metrics.

## Goal

This project is a first step toward recreating a simple version of a "With or Without You" (WOWY) style plus-minus model.

The current version estimates a player's impact by comparing:

- the average point differential in games when the player played
- versus the average point differential in games when the player did not play

This is intentionally a simple starting point before moving to more advanced adjusted models like ridge regression or RAPM-style methods.

The current WOWY implementation should be treated as a baseline, not a final player evaluation model. On pooled real NBA data it is expected to be noisy and heavily confounded by team, season, and rotation context.

## Current model

Version 1 computes:

wowy_score = average margin when player played - average margin when player did not play

Where:
- `margin` is the final point differential from one team's perspective
- `players` is the set of NBA player ids who appeared in that game for that team

This is a game-level presence model, not a possession-level or substitution-level plus-minus model.

The current WOWY CLI still reads a simple derived `games.csv` file, but the project now also defines a richer normalized game-level schema for future modeling work.

## Input data

The current WOWY program reads a derived CSV file named `games.csv`.

Expected columns:

- `game_id`
- `team`
- `margin`
- `players`

`players` should contain semicolon-separated NBA `PLAYER_ID` values.

Example:

```csv
game_id,team,margin,players
1,team_1,10,"1628369;1627759;1628401;201143;1629057"
2,team_1,6,"1628369;1627759;1628401;201143;203935"
```

## Normalized phase-1 data design

Phase 1 adds a canonical normalized layer alongside the existing WOWY CSV.

`games.csv` remains the derived compatibility format for the current WOWY CLI.

Canonical normalized tables:

- `normalized_games.csv`
- `normalized_game_players.csv`

Normalized game columns:

- `game_id`
- `season`
- `game_date`
- `team`
- `opponent`
- `is_home`
- `margin`
- `season_type`
- `source`

Normalized game-player columns:

- `game_id`
- `team`
- `player_id`
- `player_name`
- `appeared`
- `minutes`

`minutes` is included for future use but is not part of the current WOWY analysis and should not be interpreted as implemented weighting yet.

## Real NBA data

The planned real-data path uses `nba_api` to fetch NBA game-level box score data, write canonical normalized tables, and derive the same `games.csv` format above.

The WOWY model stays unchanged:

- one row per game from one team's perspective
- `margin` remains final game point differential
- `players` remains the semicolon-separated list of NBA `PLAYER_ID` values for that team

If you already have normalized game and game-player tables, derive the current WOWY input CSV with:

```bash
poetry run wowy-derive-wowy
```

Generate normalized team-season CSVs plus a derived WOWY CSV with:

```bash
poetry run wowy-ingest-nba
```

This currently defaults to `BOS`, `2023-24`, and writes:

```text
data/normalized/nba/games/BOS_2023-24.csv
data/normalized/nba/game_players/BOS_2023-24.csv
data/raw/nba/team_games/BOS_2023-24.csv
```

You can override the defaults:

```bash
poetry run wowy-ingest-nba NYK 2022-23 --csv games.csv --normalized-games-csv normalized_games.csv --normalized-game-players-csv normalized_game_players.csv --season-type "Regular Season"
```

Combine local normalized CSVs into one regression input set with:

```bash
poetry run wowy-combine-games
```

This writes:

```text
data/combined/regression/games.csv
data/combined/regression/game_players.csv
```

Run the regression analysis on those combined normalized files with:

```bash
poetry run wowy-regression
```

This now uses ridge regularization by default so the player-only game-level model remains solvable on real data. You can tune it with `--ridge-alpha`.

If you want one-step pipeline scripts, run:

```bash
poetry run python scripts/run_wowy_pipeline.py BOS:2023-24
poetry run python scripts/run_regression_pipeline.py BOS:2023-24 NYK:2023-24 --ridge-alpha 1.0
```


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

## Next model direction

The repository now includes a separate regression-based player analysis path built on normalized game-level data. The current WOWY score remains useful as a simple baseline and debugging reference, but future model development is expected to move further away from direct with-or-without averages and toward better adjusted models.
